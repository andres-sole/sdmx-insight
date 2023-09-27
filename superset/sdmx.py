from sqlalchemy import create_engine
from sdmxthon import read_sdmx
import uuid
import datetime
import yaml
import pandas as pd
from sdmxthon.webservices import webservices
from superset.connectors.sqla.models import SqlaTable
from superset.extensions import security_manager
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset.dashboards.commands.create import CreateDashboardCommand
from superset.databases.commands.create import CreateDatabaseCommand
from superset import db
import json
import re
import sqlite3
from sdmxthon.api.api import get_supported_agencies


def load_database(sdmx_url, dataset_instance=None):
    message = read_sdmx(sdmx_url)
    agency_id = get_agency_id(sdmx_url)
    dataflow_id = get_dataflow_id(sdmx_url)
    ws = None

    data = list(message.payload.values())[0].data

    supported_agencies = get_supported_agencies()
    if agency_id in supported_agencies.keys():
        ws = supported_agencies[agency_id]()
    else:
        raise Exception("Agency not supported")
    try:
        metadata = ws.get_data_flow(dataflow_id, references="descendants")
    except Exception as e:
        raise Exception(e, dataflow_id)

    df, concepts_name = generate_final_df_and_concepts_name(data, metadata.payload)
    if dataset_instance is None:
        dataset_uuid = uuid.uuid4()
        database = CreateDatabaseCommand(
            {
                "sqlalchemy_uri": f"sqlite:///dbs/{dataset_uuid}",
                "database_name": f"{dataset_uuid}",
            }
        ).run()
    else:
        dataset_uuid = dataset_instance.sdmx_uuid
        database = dataset_instance.database

    engine = create_engine(f"sqlite:///dbs/{dataset_uuid}", echo=False)

    for dataset in message.payload.keys():
        df.to_sql(str(dataset) + " " + str(datetime.datetime.now()), con=engine)

    schemas = database.get_all_schema_names(cache=False)
    tables = database.get_all_table_names_in_schema(schemas[0], force=True)

    for schema in schemas:
        security_manager.add_permission_view_menu(
            "schema_access", security_manager.get_schema_perm(database, schema)
        )

    for table in tables:
        if dataset_instance is None:
            table_instance = SqlaTable(
                table_name=f"{table[0]}",
                database=database,
                schema="main",
                is_sdmx=True,
                sdmx_url=sdmx_url,
                sdmx_uuid=dataset_uuid,
                concepts=json.dumps(concepts_name),
            )
        else:
            table_instance = dataset_instance
            table_instance.table_name = f"{table[0]}"
            db.session.add(table_instance)
            db.session.commit()

        table_instance.fetch_metadata()

    return table_instance


def create_dashboard(spec):
    dashboard = CreateDashboardCommand({"dashboard_title": spec["DashID"]}).run()

    return dashboard


def get_locale_column_if_exists(data, column, locale):
    if locale not in ["en", "es", "fr"]:
        raise Exception("Locale not supported")
    try:
        for column_data in data["columns"]:
            if column_data["column_name"] == column + f"-{locale}":
                return column + f"-{locale}"
    except Exception:
        for column_name in data.columns:
            if column_name == column + f"-{locale}":
                return column + f"-{locale}"

    return column


def get_locale_value(df, column, locale):
    if locale not in ["en", "es", "fr"]:
        raise Exception("Locale not supported")
    return df[get_locale_column_if_exists(df, column, locale)][0]


def substitute_string(string, df, locale="en"):
    pattern = r"\{\$([^\}]+)\}"
    for column in re.findall(pattern, str(string)):
        string = string.replace(
            f"{{${column}}}",
            get_locale_value(
                df,
                column,
                locale,
            ),
        )
    return string


def modify_key(original, old_key, new_key):
    return {new_key if k == old_key else k: v for k, v in original.items()}


def create_charts(spec, datasets, dashboard, locale="en"):
    for idx, row in enumerate(spec["Rows"]):
        if not row["DATA"]:
            continue

        conn = sqlite3.connect(f"dbs/{datasets[idx].sdmx_uuid}")
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        table_names = conn.execute(query).fetchall()

        # Convert list of tuples to list of strings
        table_names = [name[0] for name in table_names]

        # Query the database and load the result into a pandas DataFrame
        query = f'SELECT * FROM "{table_names[0]}"'
        df = pd.read_sql(query, conn)

        # Close the connection
        conn.close()

        mentioned_columns = ["OBS_VALUE"]  # OBS_VALUE is always available
        pattern = r"\{\$([^\}]+)\}"

        for key in list(row.keys()):  # iterating on a copy since we modify the dict
            matches = re.findall(pattern, str(row[key]))
            mentioned_columns += matches

            row[key] = substitute_string(row[key], df, locale)

        if row["chartType"] == "VALUE":
            chart = Slice(
                dashboards=[dashboard],
                datasource_id=datasets[idx].id,
                is_managed_externally=True,
                slice_name=row["Title"],
                datasource_type="table",
                viz_type="handlebars",
                params=json.dumps(
                    {
                        "datasource": f"{datasets[idx].id}__table",
                        "viz_type": "handlebars",
                        "query_mode": "raw",
                        "groupby": [],
                        "metrics": [
                            {
                                "aggregate": None,
                                "column": None,
                                "datasourceWarning": False,
                                "expressionType": "SQL",
                                "hasCustomLabel": False,
                                "label": ", ".join(mentioned_columns),
                                "sqlExpression": ", ".join(mentioned_columns),
                            }
                        ],
                        "all_columns": mentioned_columns,
                        "percent_metrics": [],
                        "order_by_cols": [],
                        "order_desc": True,
                        "row_limit": 10000,
                        "server_page_length": 10,
                        "adhoc_filters": [],
                        "handlebarsTemplate": f'{{{{#each data}}}}\n<h1 class="data-chart">\n {{{{this.OBS_VALUE}}}}\n</h1><span class="data-label">{row["Subtitle"]}</span>{{{{/each}}}}\n',
                        "styleTemplate": "\n.data-chart {\n  text-align: center;\n  font-size: 6em;\n}\n.data-label {\n  text-align: center;\n  font-size: 2em;\n  width: 100%;\n  display: block;\n}",
                        "extra_form_data": {},
                        "dashboards": [dashboard.id],
                    }
                ),
                query_context="",
            )
            dashboard.slices.append(chart)
            db.session.add(dashboard)
            db.session.add(chart)

        elif row["chartType"] == "PIE":
            chart = Slice(
                dashboards=[dashboard],
                datasource_id=datasets[idx].id,
                is_managed_externally=True,
                slice_name=row["Title"],
                datasource_type="table",
                params=json.dumps(
                    {
                        "viz_type": "pie",
                        "groupby": [
                            get_locale_column_if_exists(
                                datasets[idx].data, row["legendConcept"], locale
                            )
                        ],
                        "metric": {
                            "aggregate": None,
                            "column": None,
                            "datasourceWarning": False,
                            "expressionType": "SQL",
                            "hasCustomLabel": False,
                            "label": "OBS_VALUE",
                            "sqlExpression": "OBS_VALUE",
                        },
                        "adhoc_filters": [],
                        "row_limit": 10000,
                        "sort_by_metric": True,
                        "color_scheme": "supersetColors",
                        "show_labels_threshold": 5,
                        "show_legend": True,
                        "legendType": "scroll",
                        "legendOrientation": "right",
                        "label_type": "key_value_percent",
                        "number_format": "SMART_NUMBER",
                        "date_format": "smart_date",
                        "show_labels": True,
                        "labels_outside": True,
                        "label_line": True,
                        "outerRadius": 70,
                        "innerRadius": 30,
                        "extra_form_data": {},
                        "dashboards": [str(dashboard.id)],
                    }
                ),
                query_context="",
                viz_type="pie",
            )
            dashboard.slices.append(chart)
            db.session.add(dashboard)
            db.session.add(chart)

        elif "LINES" in row["chartType"]:
            datasets[idx].fetch_metadata()
            chart = Slice(
                dashboards=[dashboard],
                datasource_id=datasets[idx].id,
                is_managed_externally=True,
                slice_name=row["Title"],
                datasource_type="table",
                params=json.dumps(
                    {
                        "datasource": f"{datasets[idx].id}__table",
                        "viz_type": "echarts_timeseries_line",
                        "x_axis": row["xAxisConcept"],
                        "time_grain_sqla": "P1D",
                        "x_axis_sort_asc": True,
                        "x_axis_sort_series": "name",
                        "x_axis_sort_series_ascending": True,
                        "metrics": [
                            {
                                "expressionType": "SQL",
                                "sqlExpression": "OBS_VALUE",
                                "column": None,
                                "aggregate": None,
                                "datasourceWarning": False,
                                "hasCustomLabel": False,
                                "label": "OBS_VALUE",
                            }
                        ],
                        "groupby": [
                            get_locale_column_if_exists(
                                datasets[idx].data, row["legendConcept"], locale
                            )
                        ],
                        "adhoc_filters": [],
                        "order_desc": True,
                        "row_limit": 10000,
                        "truncate_metric": True,
                        "show_empty_columns": True,
                        "comparison_type": "values",
                        "annotation_layers": [],
                        "forecastPeriods": 10,
                        "forecastInterval": 0.8,
                        "x_axis_title_margin": 15,
                        "y_axis_title_margin": 15,
                        "y_axis_title_position": "Left",
                        "sort_series_type": "sum",
                        "color_scheme": "supersetColors",
                        "seriesType": "line",
                        "show_value": False,
                        "only_total": True,
                        "opacity": 0.2,
                        "markerSize": 6,
                        "zoomable": False,
                        "show_legend": True,
                        "legendType": "scroll",
                        "legendOrientation": "top",
                        "x_axis_time_format": "smart_date",
                        "rich_tooltip": True,
                        "tooltipTimeFormat": "smart_date",
                        "y_axis_format": "SMART_NUMBER",
                        "y_axis_bounds": [None, None],
                        "extra_form_data": {},
                        "dashboards": [str(dashboard.id)],
                    }
                ),
                query_context="",
                viz_type="echarts_timeseries_line",
            )
            dashboard.slices.append(chart)
            db.session.add(dashboard)
            db.session.add(chart)

        elif "BARS" in row["chartType"]:
            chart = Slice(
                dashboards=[dashboard],
                datasource_id=datasets[idx].id,
                is_managed_externally=True,
                slice_name=row["Title"],
                datasource_type="table",
                params=json.dumps(
                    {
                        "datasource": f"{datasets[idx].id}__table",
                        "viz_type": "echarts_timeseries_bar",
                        "x_axis": get_locale_column_if_exists(
                            datasets[idx].data, row["xAxisConcept"], locale
                        ),
                        "time_grain_sqla": "P1D",
                        "x_axis_sort": "OBS_VALUE",
                        "x_axis_sort_asc": False,
                        "x_axis_sort_series": "name",
                        "x_axis_sort_series_ascending": True,
                        "metrics": [
                            {
                                "expressionType": "SQL",
                                "sqlExpression": "OBS_VALUE",
                                "column": None,
                                "aggregate": None,
                                "datasourceWarning": False,
                                "hasCustomLabel": False,
                                "label": "OBS_VALUE",
                            }
                        ],
                        "groupby": [],
                        "adhoc_filters": [],
                        "order_desc": True,
                        "row_limit": 10000,
                        "truncate_metric": True,
                        "show_empty_columns": True,
                        "comparison_type": "values",
                        "annotation_layers": [],
                        "forecastPeriods": 10,
                        "forecastInterval": 0.8,
                        "orientation": "vertical",
                        "x_axis_title_margin": 15,
                        "y_axis_title_margin": 15,
                        "y_axis_title_position": "Left",
                        "sort_series_type": "sum",
                        "color_scheme": "supersetColors",
                        "only_total": True,
                        "show_legend": True,
                        "legendType": "scroll",
                        "legendOrientation": "top",
                        "x_axis_time_format": "smart_date",
                        "y_axis_format": "SMART_NUMBER",
                        "y_axis_bounds": [None, None],
                        "rich_tooltip": True,
                        "tooltipTimeFormat": "smart_date",
                        "extra_form_data": {},
                        "dashboards": [str(dashboard.id)],
                    }
                ),
                query_context="",
                viz_type="echarts_timeseries_bar",
            )

            dashboard.slices.append(chart)
            db.session.add(dashboard)
            db.session.add(chart)

        db.session.commit()

def _create_codelist_dataframe(codelist, concept_name):
    codelist_list = []
    for id, code in codelist.items.items():
        item = {'id': id}
        if isinstance(code.name, dict): 
            for lang_code, strings in code.name.items():
                item[f"{concept_name}-{lang_code}"] = strings["content"]
        else:
            item[f"{concept_name}-en"] = code.name
        codelist_list.append(item)

    return pd.DataFrame(codelist_list)


def _generate_insight_dict(metadata_payload):
    result = {}
    codelists = metadata_payload['Codelists']
    structure = metadata_payload['DataStructures']

    if len(structure) !=  1:
        raise Exception('One structure expected')
    structure = list(structure.values())[0]
    for id, component in structure.dimension_descriptor.components.items():
        codelist=component.representation.codelist
        if codelist:
            codelist = _create_codelist_dataframe(codelist, component.id)
        result[id] = {'name': component.concept_identity.name, 'codelist': codelist}
    return result


def generate_final_df_and_concepts_name(data, metadata_payload):
    insight_dict = _generate_insight_dict(metadata_payload)
    
    concepts_names = {}
    for code, component in insight_dict.items():
        concepts_names[code] = component['name']
        if component['codelist'] is not None:
            data = data.merge(component['codelist'], left_on=code, right_on='id', how='inner')
            data.drop(columns=['id_x', 'id_y'], inplace=True, errors='ignore')

    return data, concepts_names

def get_dataflow_id(sdmx_url):
    full_dataflow_id = sdmx_url.split("data/")[1].split("/")[0]
    if "," in full_dataflow_id:
        dataflow_id = full_dataflow_id.split(",")[1]
    else:
        dataflow_id = full_dataflow_id
    return dataflow_id


def get_agency_id(sdmx_url):
    if "stats.bis.org" in sdmx_url:
        return "BIS"
    elif "ecb.europa.eu" in sdmx_url:
        return "ECB"
    elif "ec.europa.eu" in sdmx_url:
        return "ESTAT"
    elif "ilo.org" in sdmx_url:
        return "ILO"
    elif "oecd.org" in sdmx_url:
        return "OECD"
    elif "unicef.org" in sdmx_url:
        return "UNICEF"    
