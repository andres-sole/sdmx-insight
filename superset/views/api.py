# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import Any, TYPE_CHECKING

from flask import request
from flask_appbuilder import expose
from flask_appbuilder.api import rison
from flask_appbuilder.security.decorators import has_access_api
from flask_babel import lazy_gettext as _

from superset import db, event_logger
from superset.commands.chart.exceptions import (
    TimeRangeAmbiguousError,
    TimeRangeParseFailError,
)
from superset.legacy import update_time_range
from superset.models.slice import Slice
from superset.models.core import Database
from superset.superset_typing import FlaskResponse
from superset.utils import json
from superset.utils.date_parser import get_since_until
from superset.views.error_handling import handle_api_exception
from superset.views.base import api, BaseSupersetView
from sqlalchemy import create_engine
from sdmxthon import read_sdmx
import uuid
import datetime
import requests
from superset.views.base import api, BaseSupersetView, handle_api_exception
from superset.sdmx import load_database, create_dashboard, create_charts
import yaml
from sdmxthon.api.api import get_supported_agencies
from sdmxthon.webservices import webservices

if TYPE_CHECKING:
    from superset.common.query_context_factory import QueryContextFactory

get_time_range_schema = {
    "type": ["string", "array"],
    "items": {
        "type": "object",
        "properties": {
            "timeRange": {"type": "string"},
            "shift": {"type": "string"},
        },
    },
}


class Api(BaseSupersetView):
    query_context_factory = None

    # @has_access_api
    @event_logger.log_this
    @api
    @handle_api_exception
    @expose("/v1/sdmx/dashboard", methods=("POST",))
    def sdmx_upload_dashboard(self) -> FlaskResponse:
        try:
            uploaded_file = request.files["file"]
            file_content = uploaded_file.read()
            spec = yaml.safe_load(file_content)
            locale = request.form.get("locale", "en")
            datasets = []

            for row in spec["Rows"]:
                if not row["DATA"]:
                    datasets.append(None)
                    continue
                sdmx_url = row["DATA"].split(" ")[0]
                datasets.append(load_database(sdmx_url))

            dashboard = create_dashboard(spec)

            create_charts(spec, datasets, dashboard, locale)

            return self.json_response({"status": "OK", "dashboard_id": dashboard.id})
        except Exception as e:
            return self.json_response(
                json.dumps({"error": str(e)}),
                status=500,
            )

    # @has_access_api
    @event_logger.log_this
    @api
    @handle_api_exception
    @expose("/v1/sdmx/", methods=("POST",))
    def sdmx_upload(self) -> FlaskResponse:
        try:
            json_data = request.json
            is_raw_url = False
            if "sdmxUrl" in json_data:
                sdmx_url = json_data["sdmxUrl"]
                is_raw_url = True
            elif "agencyId" in json_data:
                agency_id = json_data["agencyId"]
                supported_agencies = get_supported_agencies()
                if json_data["agencyId"] in supported_agencies.keys():
                    dataflow_id = json_data["dataflowId"].split(":")[1].split("(")[0]
                    sdmx_url = supported_agencies[agency_id]().get_data_url(dataflow_id, last_n_observations=json_data["numberOfObservations"])
                else:
                    sdmx_url = json_data["sdmxUrl"]
            load_database(sdmx_url, is_raw_url=is_raw_url)
            return self.json_response({"status": "OK"})
        except Exception as e:
            return self.json_response(
                json.dumps({"error": str(e)}),
                status=500,
            )

    @event_logger.log_this
    @api
    @handle_api_exception
    @expose("/v1/sdmx/agency", methods=("GET",))
    def sdmx_get_agencies(self) -> FlaskResponse:
        try:
            agencies = list(get_supported_agencies().keys())
            return self.json_response(agencies)
        except Exception as e:
            return self.json_response(
                json.dumps({"error": str(e)}),
                status=500,
            )

    @event_logger.log_this
    @api
    @handle_api_exception
    @expose("/v1/sdmx/agency/<agency_id>", methods=("GET",))
    def sdmx_get_dataflows(self, agency_id) -> FlaskResponse:
        try:
            return self.json_response(
                get_supported_agencies()[agency_id]().get_all_dataflows()
            )

        except Exception as e:
            return self.json_response(
                json.dumps({"error": str(e)}),
                status=500,
            )

    @event_logger.log_this
    @api
    @handle_api_exception
    @has_access_api
    @expose("/v1/query/", methods=("POST",))
    def query(self) -> FlaskResponse:
        """
        Take a query_obj constructed in the client and returns payload data response
        for the given query_obj.

        raises SupersetSecurityException: If the user cannot access the resource
        """
        query_context = self.get_query_context_factory().create(
            **json.loads(request.form["query_context"])
        )
        query_context.raise_for_access()
        result = query_context.get_payload()
        payload_json = result["queries"]
        return json.dumps(payload_json, default=json.json_int_dttm_ser, ignore_nan=True)

    @event_logger.log_this
    @api
    @handle_api_exception
    @has_access_api
    @expose("/v1/form_data/", methods=("GET",))
    def query_form_data(self) -> FlaskResponse:
        """
        Get the form_data stored in the database for existing slice.
        params: slice_id: integer
        """
        form_data = {}
        if slice_id := request.args.get("slice_id"):
            slc = db.session.query(Slice).filter_by(id=slice_id).one_or_none()
            if slc:
                form_data = slc.form_data.copy()

        update_time_range(form_data)

        return self.json_response(form_data)

    @api
    @handle_api_exception
    @has_access_api
    @rison(get_time_range_schema)
    @expose("/v1/time_range/", methods=("GET",))
    def time_range(self, **kwargs: Any) -> FlaskResponse:
        """Get actually time range from human-readable string or datetime expression."""
        time_ranges = kwargs["rison"]
        try:
            if isinstance(time_ranges, str):
                time_ranges = [{"timeRange": time_ranges}]

            rv = []
            for time_range in time_ranges:
                since, until = get_since_until(
                    time_range=time_range["timeRange"],
                    time_shift=time_range.get("shift"),
                )
                rv.append(
                    {
                        "since": since.isoformat() if since else "",
                        "until": until.isoformat() if until else "",
                        "timeRange": time_range["timeRange"],
                        "shift": time_range.get("shift"),
                    }
                )
            return self.json_response({"result": rv})
        except (ValueError, TimeRangeParseFailError, TimeRangeAmbiguousError) as error:
            error_msg = {"message": _("Unexpected time range: %(error)s", error=error)}
            return self.json_response(error_msg, 400)

    def get_query_context_factory(self) -> QueryContextFactory:
        if self.query_context_factory is None:
            # pylint: disable=import-outside-toplevel
            from superset.common.query_context_factory import QueryContextFactory

            self.query_context_factory = QueryContextFactory()
        return self.query_context_factory
