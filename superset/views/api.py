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
import yaml

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

def load_databases(spec, base_url, tokens):
    result = []
    for row in spec["Rows"]:
        if not row["DATA"]:
            result.append(None)
            continue

        sdmx_url = row["DATA"].split(" ")[0]

        message = read_sdmx(sdmx_url)
        dataset_uuid = uuid.uuid4()

        engine = create_engine(f"sqlite:///dbs/{dataset_uuid}", echo=False)

        for dataset in message.payload.keys():
            df = message.payload[dataset].data
            df.to_sql(str(dataset) + " " + str(datetime.datetime.now()), con=engine)

        response = requests.post(
            base_url + "api/v1/database",
            json={
                "sqlalchemy_uri": f"sqlite:///dbs/{dataset_uuid}",
                "database_name": f"{dataset_uuid}",
            },
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )

        database_id = response.json()["id"]

        tables = requests.get(
            base_url + f"api/v1/database/{database_id}/tables",
            params={"q": f"(schema_name:main)"},
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        ).json()["result"]

        for table in tables:
            response = requests.post(
                base_url + "api/v1/dataset",
                json={
                    "table_name": f"{table['value']}",
                    "database": f"{database_id}",
                    "schema": "main",
                    "is_sdmx": True,
                    "sdmx_url": sdmx_url,
                },
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )
            result.append(response.json()["data"])
    return result


def create_dashboard(spec, base_url, tokens):
    response = requests.post(
        base_url + "api/v1/dashboard/",
        json={
            "certification_details": None,
            "certified_by": None,
            "css": "",
            "dashboard_title": spec["DashID"],
            "external_url": None,
            "is_managed_externally": True,
            "json_metadata": "",
            "owners": [1],
            "position_json": "",
            "published": False,
            "roles": [1],
            "slug": None,
        },
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )

    return response.json()["data"] if "data" in response.json() else response.json()


def create_charts(spec, datasets, dashboard, base_url, tokens):
    
    for idx, row in enumerate(spec["Rows"]):
        if not row["DATA"]:
            continue

        if row["chartType"] == "VALUE":
            response = requests.post(
                base_url + "api/v1/chart/",
                json={
                    "cache_timeout": 0,
                    "certification_details": None,
                    "certified_by": None,
                    "dashboards": [ dashboard["id"] ],
                    "description": None,
                    "is_managed_externally": True,
                    "owners": [1],
                    "datasource_id": datasets[idx]["id"],
                    "params": "{\"datasource\":\"" + str(datasets[idx]["id"]) + "__table\",\"viz_type\":\"handlebars\",\"query_mode\":\"aggregate\",\"groupby\":[],\"metrics\":[{\"aggregate\":null,\"column\":null,\"datasourceWarning\":false,\"expressionType\":\"SQL\",\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"\",\"sqlExpression\":\"OBS_VALUE\"}],\"all_columns\":[],\"percent_metrics\":[],\"order_by_cols\":[],\"order_desc\":true,\"row_limit\":10000,\"server_page_length\":10,\"adhoc_filters\":[],\"handlebarsTemplate\":\"<h1 class=\\\"data-chart\\\">\\n  {{#each data}}\\n    {{this.OBS_VALUE}}</li>\\n  {{/each}}\\n</h1>\",\"styleTemplate\":\"\\n.data-chart {\\n  text-align: center;\\n  font-size: 6em;\\n}\\n\",\"extra_form_data\":{},\"dashboards\":[" + str(dashboard['id']) +"]}",
                    "query_context": "{\"datasource\":{\"id\": " + str(datasets[idx]["id"]) + ",\"type\":\"table\"},\"force\":false,\"queries\":[{\"filters\":[],\"extras\":{\"having\":\"\",\"where\":\"\"},\"applied_time_extras\":{},\"columns\":[],\"metrics\":[{\"aggregate\":null,\"column\":null,\"datasourceWarning\":false,\"expressionType\":\"SQL\",\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"\",\"sqlExpression\":\"OBS_VALUE\"}],\"orderby\":[[{\"aggregate\":null,\"column\":null,\"datasourceWarning\":false,\"expressionType\":\"SQL\",\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"\",\"sqlExpression\":\"OBS_VALUE\"},false]],\"annotation_layers\":[],\"row_limit\":10000,\"series_limit\":0,\"order_desc\":true,\"url_params\":{},\"custom_params\":{},\"custom_form_data\":{}}],\"form_data\":{\"datasource\":\"" + str(datasets[idx]["id"])  + "__table\",\"viz_type\":\"handlebars\",\"query_mode\":\"aggregate\",\"groupby\":[],\"metrics\":[{\"aggregate\":null,\"column\":null,\"datasourceWarning\":false,\"expressionType\":\"SQL\",\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"\",\"sqlExpression\":\"OBS_VALUE\"}],\"all_columns\":[],\"percent_metrics\":[],\"order_by_cols\":[],\"order_desc\":true,\"row_limit\":10000,\"server_page_length\":10,\"adhoc_filters\":[],\"handlebarsTemplate\":\"<h1 class=\\\"data-chart\\\">\\n  {{#each data}}\\n    {{this.OBS_VALUE}}</li>\\n  {{/each}}\\n</h1>\",\"styleTemplate\":\"\\n.data-chart {\\n  text-align: center;\\n  font-size: 6em;\\n}\\n\",\"extra_form_data\":{},\"dashboards\":[40],\"force\":false,\"result_format\":\"json\",\"result_type\":\"full\"},\"result_format\":\"json\",\"result_type\":\"full\"}",
                    "slice_name": row["Title"],
                    "datasource_type": "table",
                    "viz_type": "handlebars",
                },
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )
        elif row["chartType"] == "PIE":
            response = requests.post(
                base_url + "api/v1/chart/",
                json={
                    "cache_timeout": 0,
                    "certification_details": None,
                    "certified_by": None,
                    "dashboards": [dashboard["id"]],
                    "description": None,
                    "is_managed_externally": True,
                    "owners": [1],
                    "datasource_id": datasets[idx]["id"],
                    "params": "{\"viz_type\":\"pie\",\"groupby\":[\"" + row['legendConcept'] + "\"],\"metric\":{\"aggregate\":null,\"column\":null,\"datasourceWarning\":false,\"expressionType\":\"SQL\",\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_2o7qsp2myck_m08ex1fqyyp\",\"sqlExpression\":\"OBS_VALUE\"},\"adhoc_filters\":[],\"row_limit\":10000,\"sort_by_metric\":true,\"color_scheme\":\"supersetColors\",\"show_labels_threshold\":5,\"show_legend\":true,\"legendType\":\"scroll\",\"legendOrientation\":\"right\",\"label_type\":\"key_value_percent\",\"number_format\":\"SMART_NUMBER\",\"date_format\":\"smart_date\",\"show_labels\":true,\"labels_outside\":true,\"label_line\":true,\"outerRadius\":70,\"innerRadius\":30,\"extra_form_data\":{},\"dashboards\":[" + str(dashboard["id"]) + "]}",
                    "query_context": "",
                    "slice_name": row["Title"],
                    "datasource_type": "table",
                    "viz_type": "pie",
                },
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )
        elif "LINES" in row["chartType"]:
            response = requests.post(
                base_url + "api/v1/chart/",
                json={
                    "cache_timeout": 0,
                    "certification_details": None,
                    "certified_by": None,
                    "dashboards": [dashboard["id"]],
                    "description": None,
                    "is_managed_externally": True,
                    "owners": [1],
                    "datasource_id": datasets[idx]["id"],
                    "params": "{\"datasource\":\"" + str(datasets[idx]['id']) + "__table\",\"viz_type\":\"echarts_timeseries_line\",\"x_axis\":\"" + row["xAxisConcept"] + "\",\"time_grain_sqla\":\"P1D\",\"x_axis_sort_asc\":true,\"x_axis_sort_series\":\"name\",\"x_axis_sort_series_ascending\":true,\"metrics\":[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_2pzqjh4ddnr_9fsj62hsc9c\"}],\"groupby\":[\"SEX\"],\"adhoc_filters\":[],\"order_desc\":true,\"row_limit\":10000,\"truncate_metric\":true,\"show_empty_columns\":true,\"comparison_type\":\"values\",\"annotation_layers\":[],\"forecastPeriods\":10,\"forecastInterval\":0.8,\"x_axis_title_margin\":15,\"y_axis_title_margin\":15,\"y_axis_title_position\":\"Left\",\"sort_series_type\":\"sum\",\"color_scheme\":\"supersetColors\",\"seriesType\":\"line\",\"show_value\":false,\"only_total\":true,\"opacity\":0.2,\"markerSize\":6,\"zoomable\":false,\"show_legend\":true,\"legendType\":\"scroll\",\"legendOrientation\":\"top\",\"x_axis_time_format\":\"smart_date\",\"rich_tooltip\":true,\"tooltipTimeFormat\":\"smart_date\",\"y_axis_format\":\"SMART_NUMBER\",\"y_axis_bounds\":[null,null],\"extra_form_data\":{},\"dashboards\":[1]}",
                    "query_context": "{\"datasource\":{\"id\":" +  str(datasets[idx]['id'])  + ",\"type\":\"table\"},\"force\":false,\"queries\":[{\"filters\":[],\"extras\":{\"having\":\"\",\"where\":\"\"},\"applied_time_extras\":{},\"columns\":[{\"timeGrain\":\"P1D\",\"columnType\":\"BASE_AXIS\",\"sqlExpression\":\"" + row["xAxisConcept"] + "\",\"label\":\"" + row["xAxisConcept"] + "\",\"expressionType\":\"SQL\"},\"SEX\"],\"metrics\":[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_2pzqjh4ddnr_9fsj62hsc9c\"}],\"orderby\":[[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_2pzqjh4ddnr_9fsj62hsc9c\"},false]],\"annotation_layers\":[],\"row_limit\":10000,\"series_columns\":[\"SEX\"],\"series_limit\":0,\"order_desc\":true,\"url_params\":{},\"custom_params\":{},\"custom_form_data\":{},\"time_offsets\":[],\"post_processing\":[{\"operation\":\"pivot\",\"options\":{\"index\":[\"" + row["xAxisConcept"] + "\"],\"columns\":[\"SEX\"],\"aggregates\":{\"OBS_VALUE\":{\"operator\":\"mean\"}},\"drop_missing_columns\":false}},{\"operation\":\"rename\",\"options\":{\"columns\":{\"OBS_VALUE\":null},\"level\":0,\"inplace\":true}},{\"operation\":\"flatten\"}]}],\"form_data\":{\"datasource\":\"9__table\",\"viz_type\":\"echarts_timeseries_line\",\"x_axis\":\"" + row["xAxisConcept"] + "\",\"time_grain_sqla\":\"P1D\",\"x_axis_sort_asc\":true,\"x_axis_sort_series\":\"name\",\"x_axis_sort_series_ascending\":true,\"metrics\":[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_2pzqjh4ddnr_9fsj62hsc9c\"}],\"groupby\":[\"SEX\"],\"adhoc_filters\":[],\"order_desc\":true,\"row_limit\":10000,\"truncate_metric\":true,\"show_empty_columns\":true,\"comparison_type\":\"values\",\"annotation_layers\":[],\"forecastPeriods\":10,\"forecastInterval\":0.8,\"x_axis_title_margin\":15,\"y_axis_title_margin\":15,\"y_axis_title_position\":\"Left\",\"sort_series_type\":\"sum\",\"color_scheme\":\"supersetColors\",\"seriesType\":\"line\",\"show_value\":false,\"only_total\":true,\"opacity\":0.2,\"markerSize\":6,\"zoomable\":false,\"show_legend\":true,\"legendType\":\"scroll\",\"legendOrientation\":\"top\",\"x_axis_time_format\":\"smart_date\",\"rich_tooltip\":true,\"tooltipTimeFormat\":\"smart_date\",\"y_axis_format\":\"SMART_NUMBER\",\"y_axis_bounds\":[null,null],\"extra_form_data\":{},\"dashboards\":[ " + str(dashboard['id']) + " ],\"force\":false,\"result_format\":\"json\",\"result_type\":\"full\"},\"result_format\":\"json\",\"result_type\":\"full\"}",
                    "query_context": "",
                    "slice_name": row["Title"],
                    "datasource_type": "table",
                    "viz_type": "echarts_timeseries_line",
                },
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )
        elif "BARS" in row["chartType"]:
            response = requests.post(
                base_url + "api/v1/chart/",
                json={
                    "cache_timeout": 0,
                    "certification_details": None,
                    "certified_by": None,
                    "dashboards": [dashboard["id"]],
                    "description": None,
                    "is_managed_externally": True,
                    "owners": [1],
                    "datasource_id": datasets[idx]["id"],
                    "params": "{\"datasource\":\"" + str(datasets[idx]["id"]) + "__table\",\"viz_type\":\"echarts_timeseries_bar\",\"x_axis\":\"" + row['xAxisConcept'] + "\",\"time_grain_sqla\":\"P1D\",\"x_axis_sort\":\"OBS_VALUE\",\"x_axis_sort_asc\":false,\"x_axis_sort_series\":\"name\",\"x_axis_sort_series_ascending\":true,\"metrics\":[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_qll9scn97y_rj89e0zvnw\"}],\"groupby\":[],\"adhoc_filters\":[],\"order_desc\":true,\"row_limit\":10000,\"truncate_metric\":true,\"show_empty_columns\":true,\"comparison_type\":\"values\",\"annotation_layers\":[],\"forecastPeriods\":10,\"forecastInterval\":0.8,\"orientation\":\"vertical\",\"x_axis_title_margin\":15,\"y_axis_title_margin\":15,\"y_axis_title_position\":\"Left\",\"sort_series_type\":\"sum\",\"color_scheme\":\"supersetColors\",\"only_total\":true,\"show_legend\":true,\"legendType\":\"scroll\",\"legendOrientation\":\"top\",\"x_axis_time_format\":\"smart_date\",\"y_axis_format\":\"SMART_NUMBER\",\"y_axis_bounds\":[null,null],\"rich_tooltip\":true,\"tooltipTimeFormat\":\"smart_date\",\"extra_form_data\":{},\"dashboards\":[]}",
                    "query_context": "{\"datasource\":{\"id\":" + str(datasets[idx]["id"]) + ",\"type\":\"table\"},\"force\":false,\"queries\":[{\"filters\":[],\"extras\":{\"having\":\"\",\"where\":\"\"},\"applied_time_extras\":{},\"columns\":[{\"timeGrain\":\"P1D\",\"columnType\":\"BASE_AXIS\",\"sqlExpression\":\"" + row['xAxisConcept'] + "\",\"label\":\"" + row['xAxisConcept'] + "\",\"expressionType\":\"SQL\"}],\"metrics\":[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_qll9scn97y_rj89e0zvnw\"}],\"orderby\":[[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_qll9scn97y_rj89e0zvnw\"},false]],\"annotation_layers\":[],\"row_limit\":10000,\"series_columns\":[],\"series_limit\":0,\"order_desc\":true,\"url_params\":{},\"custom_params\":{},\"custom_form_data\":{},\"time_offsets\":[],\"post_processing\":[{\"operation\":\"pivot\",\"options\":{\"index\":[\"" + row['xAxisConcept'] + "\"],\"columns\":[],\"aggregates\":{\"OBS_VALUE\":{\"operator\":\"mean\"}},\"drop_missing_columns\":false}},{\"operation\":\"sort\",\"options\":{\"by\":\"OBS_VALUE\",\"ascending\":false}},{\"operation\":\"flatten\"}]}],\"form_data\":{\"datasource\":\"" + str(datasets[idx]["id"]) + "__table\",\"viz_type\":\"echarts_timeseries_bar\",\"x_axis\":\"" + row['xAxisConcept'] + "\",\"time_grain_sqla\":\"P1D\",\"x_axis_sort\":\"OBS_VALUE\",\"x_axis_sort_asc\":false,\"x_axis_sort_series\":\"name\",\"x_axis_sort_series_ascending\":true,\"metrics\":[{\"expressionType\":\"SQL\",\"sqlExpression\":\"OBS_VALUE\",\"column\":null,\"aggregate\":null,\"datasourceWarning\":false,\"hasCustomLabel\":false,\"label\":\"OBS_VALUE\",\"optionName\":\"metric_qll9scn97y_rj89e0zvnw\"}],\"groupby\":[],\"adhoc_filters\":[],\"order_desc\":true,\"row_limit\":10000,\"truncate_metric\":true,\"show_empty_columns\":true,\"comparison_type\":\"values\",\"annotation_layers\":[],\"forecastPeriods\":10,\"forecastInterval\":0.8,\"orientation\":\"vertical\",\"x_axis_title_margin\":15,\"y_axis_title_margin\":15,\"y_axis_title_position\":\"Left\",\"sort_series_type\":\"sum\",\"color_scheme\":\"supersetColors\",\"only_total\":true,\"show_legend\":true,\"legendType\":\"scroll\",\"legendOrientation\":\"top\",\"x_axis_time_format\":\"smart_date\",\"y_axis_format\":\"SMART_NUMBER\",\"y_axis_bounds\":[null,null],\"rich_tooltip\":true,\"tooltipTimeFormat\":\"smart_date\",\"extra_form_data\":{},\"dashboards\":["  + str(dashboard['id']) + "],\"force\":false,\"result_format\":\"json\",\"result_type\":\"full\"},\"result_format\":\"json\",\"result_type\":\"full\"}",
                    "query_context": "",
                    "slice_name": row["Title"],
                    "datasource_type": "table",
                    "viz_type": "echarts_timeseries_bar",
                },
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )

class Api(BaseSupersetView):
    query_context_factory = None

    @event_logger.log_this
    @api
    @handle_api_exception
    @expose("/v1/sdmx/dashboard", methods=("POST",))
    def sdmx_upload_dashboard(self) -> FlaskResponse:
        try:
            uploaded_file = request.files['file']
            file_content = uploaded_file.read()
            spec = yaml.safe_load(file_content)
            base_url = request.url_root

            tokens = requests.post(
                base_url + "api/v1/security/login",
                json={
                    "password": "admin",
                    "provider": "db",
                    "refresh": False,
                    "username": "admin",
                },
            ).json()

            datasets = load_databases(spec, base_url, tokens)

            dashboard = create_dashboard(spec, base_url, tokens)

            create_charts(spec, datasets, dashboard, base_url, tokens)

            return self.json_response({"status": "OK"})
        except Exception as e:
            raise e
            return self.json_response(
                json.dumps({'error': str(e)}),
                status=500,
            )

    @event_logger.log_this
    @api
    @handle_api_exception
    @expose("/v1/sdmx/", methods=("POST",))
    def sdmx_upload(self) -> FlaskResponse:
        try:
            data = request.json
            sdmx_url = data["sdmxUrl"]
            base_url = request.url_root

            message = read_sdmx(
                sdmx_url
            )

            dataset_uuid = uuid.uuid4()

            engine = create_engine(f"sqlite:///dbs/{dataset_uuid}", echo=False)

            for dataset in message.payload.keys():
                df = message.payload[dataset].data
                df.to_sql(str(dataset) + " " + str(datetime.datetime.now()), con=engine)

            tokens = requests.post(
                base_url + "api/v1/security/login",
                json={
                    "password": "admin",
                    "provider": "db",
                    "refresh": False,
                    "username": "admin",
                },
            ).json()

            response = requests.post(
                base_url + "api/v1/database/",
                json={
                    "sqlalchemy_uri": f"sqlite:///dbs/{dataset_uuid}",
                    "database_name": f"{dataset_uuid}",
                },
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )

            database_id = response.json()["id"]

            tables = requests.get(
                base_url + f"/api/v1/database/{database_id}/tables/?q=(schema_name:main)",
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            ).json()["result"]

            datasets = []
            for table in tables:
                response = requests.post(
                    base_url + "/api/v1/dataset/",
                    json={
                        "table_name": f"{table['value']}",
                        "database": f"{database_id}",
                        "schema": "main",
                        "is_sdmx": True,
                        "sdmx_url": sdmx_url,
                    },
                    headers={"Authorization": f"Bearer {tokens['access_token']}"},
                )
                datasets.append(
                    {
                        "dataset_id": response.json()["id"],
                        "table_name": response.json()["result"]["table_name"],
                    }
                )
            if not sdmx_url:
                return self.json_response(
                    {'error': 'sdmxUrl is required'},
                    status=400,
                )
            return self.json_response({"status": "OK"})
        except Exception as e:
            return self.json_response(
                json.dumps({'error': str(e)}),
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
