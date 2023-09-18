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


def load_database(sdmx_url, dataset_instance=None):
    message = read_sdmx(sdmx_url)

    dataflow_id = get_dataflow_id(sdmx_url)
    agency_id = get_agency_id(sdmx_url)
    ws = None

    if agency_id == "BIS":
        ws = webservices.BisWs()
    elif agency_id == "ECB":
        ws = webservices.EcbWs()
    elif agency_id == "ESTAT":
        ws = webservices.EstatWs()
    elif agency_id == "ILO":
        ws = webservices.IloWs()
    try:
        metadata = ws.get_data_flow(dataflow_id, references="descendants")
    except Exception:
        raise Exception(dataflow_id)

    data = list(message.payload.values())[0].data
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

    for column_data in data["columns"]:
        if column_data["column_name"] == column + f"-{locale}":
            return column + f"-{locale}"
    
    return column


def create_charts(spec, datasets, dashboard, locale="en"):
    for idx, row in enumerate(spec["Rows"]):
        if not row["DATA"]:
            continue

        if row["chartType"] == "VALUE":
            chart = Slice(
                dashboards=[dashboard],
                datasource_id=datasets[idx].id,
                is_managed_externally=True,
                slice_name=row["Title"],
                datasource_type="table",
                params='{"datasource":"'
                + str(datasets[idx].id)
                + '__table","viz_type":"handlebars","query_mode":"aggregate","groupby":[],"metrics":[{"aggregate":null,"column":null,"datasourceWarning":false,"expressionType":"SQL","hasCustomLabel":false,"label":"OBS_VALUE","optionName":"","sqlExpression":"OBS_VALUE"}],"all_columns":[],"percent_metrics":[],"order_by_cols":[],"order_desc":true,"row_limit":10000,"server_page_length":10,"adhoc_filters":[],"handlebarsTemplate":"<h1 class=\\"data-chart\\">\\n  {{#each data}}\\n    {{this.OBS_VALUE}}</li>\\n  {{/each}}\\n</h1>","styleTemplate":"\\n.data-chart {\\n  text-align: center;\\n  font-size: 6em;\\n}\\n","extra_form_data":{},"dashboards":['
                + str(dashboard.id)
                + "]}",
                query_context='{"datasource":{"id": '
                + str(datasets[idx].id)
                + ',"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},"columns":[],"metrics":[{"aggregate":null,"column":null,"datasourceWarning":false,"expressionType":"SQL","hasCustomLabel":false,"label":"OBS_VALUE","optionName":"","sqlExpression":"OBS_VALUE"}],"orderby":[[{"aggregate":null,"column":null,"datasourceWarning":false,"expressionType":"SQL","hasCustomLabel":false,"label":"OBS_VALUE","optionName":"","sqlExpression":"OBS_VALUE"},false]],"annotation_layers":[],"row_limit":10000,"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{}}],"form_data":{"datasource":"'
                + str(datasets[idx].id)
                + '__table","viz_type":"handlebars","query_mode":"aggregate","groupby":[],"metrics":[{"aggregate":null,"column":null,"datasourceWarning":false,"expressionType":"SQL","hasCustomLabel":false,"label":"OBS_VALUE","optionName":"","sqlExpression":"OBS_VALUE"}],"all_columns":[],"percent_metrics":[],"order_by_cols":[],"order_desc":true,"row_limit":10000,"server_page_length":10,"adhoc_filters":[],"handlebarsTemplate":"<h1 class=\\"data-chart\\">\\n  {{#each data}}\\n    {{this.OBS_VALUE}}</li>\\n  {{/each}}\\n</h1>","styleTemplate":"\\n.data-chart {\\n  text-align: center;\\n  font-size: 6em;\\n}\\n","extra_form_data":{},"dashboards":[40],"force":false,"result_format":"json","result_type":"full"},"result_format":"json","result_type":"full"}',
                viz_type="handlebars",
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
                params='{"viz_type":"pie","groupby":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["legendConcept"], locale
                )
                + '"],"metric":{"aggregate":null,"column":null,"datasourceWarning":false,"expressionType":"SQL","hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_2o7qsp2myck_m08ex1fqyyp","sqlExpression":"OBS_VALUE"},"adhoc_filters":[],"row_limit":10000,"sort_by_metric":true,"color_scheme":"supersetColors","show_labels_threshold":5,"show_legend":true,"legendType":"scroll","legendOrientation":"right","label_type":"key_value_percent","number_format":"SMART_NUMBER","date_format":"smart_date","show_labels":true,"labels_outside":true,"label_line":true,"outerRadius":70,"innerRadius":30,"extra_form_data":{},"dashboards":['
                + str(dashboard.id)
                + "]}",
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
                params='{"datasource":"'
                + str(datasets[idx].id)
                + '__table","viz_type":"echarts_timeseries_line","x_axis":"'
                + row["xAxisConcept"]
                + '","time_grain_sqla":"P1D","x_axis_sort_asc":true,"x_axis_sort_series":"name","x_axis_sort_series_ascending":true,"metrics":[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_2pzqjh4ddnr_9fsj62hsc9c"}],"groupby":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["legendConcept"], locale
                )
                + '"],"adhoc_filters":[],"order_desc":true,"row_limit":10000,"truncate_metric":true,"show_empty_columns":true,"comparison_type":"values","annotation_layers":[],"forecastPeriods":10,"forecastInterval":0.8,"x_axis_title_margin":15,"y_axis_title_margin":15,"y_axis_title_position":"Left","sort_series_type":"sum","color_scheme":"supersetColors","seriesType":"line","show_value":false,"only_total":true,"opacity":0.2,"markerSize":6,"zoomable":false,"show_legend":true,"legendType":"scroll","legendOrientation":"top","x_axis_time_format":"smart_date","rich_tooltip":true,"tooltipTimeFormat":"smart_date","y_axis_format":"SMART_NUMBER","y_axis_bounds":[null,null],"extra_form_data":{},"dashboards":['
                + str(dashboard.id)
                + "]}",
                query_context='{"datasource":{"id":'
                + str(datasets[idx].id)
                + ',"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},"columns":[{"timeGrain":"P1D","columnType":"BASE_AXIS","sqlExpression":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","label":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","expressionType":"SQL"},"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["legendConcept"], locale
                )
                + '"],"metrics":[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_2pzqjh4ddnr_9fsj62hsc9c"}],"orderby":[[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_2pzqjh4ddnr_9fsj62hsc9c"},false]],"annotation_layers":[],"row_limit":10000,"series_columns":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["legendConcept"], locale
                )
                + '"],"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{},"time_offsets":[],"post_processing":[{"operation":"pivot","options":{"index":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '"],"columns":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["legendConcept"], locale
                )
                + '"],"aggregates":{"OBS_VALUE":{"operator":"mean"}},"drop_missing_columns":false}},{"operation":"rename","options":{"columns":{"OBS_VALUE":null},"level":0,"inplace":true}},{"operation":"flatten"}]}],"form_data":{"datasource":"9__table","viz_type":"echarts_timeseries_line","x_axis":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","time_grain_sqla":"P1D","x_axis_sort_asc":true,"x_axis_sort_series":"name","x_axis_sort_series_ascending":true,"metrics":[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_2pzqjh4ddnr_9fsj62hsc9c"}],"groupby":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["legendConcept"], locale
                )
                + '"],"adhoc_filters":[],"order_desc":true,"row_limit":10000,"truncate_metric":true,"show_empty_columns":true,"comparison_type":"values","annotation_layers":[],"forecastPeriods":10,"forecastInterval":0.8,"x_axis_title_margin":15,"y_axis_title_margin":15,"y_axis_title_position":"Left","sort_series_type":"sum","color_scheme":"supersetColors","seriesType":"line","show_value":false,"only_total":true,"opacity":0.2,"markerSize":6,"zoomable":false,"show_legend":true,"legendType":"scroll","legendOrientation":"top","x_axis_time_format":"smart_date","rich_tooltip":true,"tooltipTimeFormat":"smart_date","y_axis_format":"SMART_NUMBER","y_axis_bounds":[null,null],"extra_form_data":{},"dashboards":[ '
                + str(dashboard.id)
                + ' ],"force":false,"result_format":"json","result_type":"full"},"result_format":"json","result_type":"full"}',
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
                params='{"datasource":"'
                + str(datasets[idx].id)
                + '__table","viz_type":"echarts_timeseries_bar","x_axis":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","time_grain_sqla":"P1D","x_axis_sort":"OBS_VALUE","x_axis_sort_asc":false,"x_axis_sort_series":"name","x_axis_sort_series_ascending":true,"metrics":[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_qll9scn97y_rj89e0zvnw"}],"groupby":[],"adhoc_filters":[],"order_desc":true,"row_limit":10000,"truncate_metric":true,"show_empty_columns":true,"comparison_type":"values","annotation_layers":[],"forecastPeriods":10,"forecastInterval":0.8,"orientation":"vertical","x_axis_title_margin":15,"y_axis_title_margin":15,"y_axis_title_position":"Left","sort_series_type":"sum","color_scheme":"supersetColors","only_total":true,"show_legend":true,"legendType":"scroll","legendOrientation":"top","x_axis_time_format":"smart_date","y_axis_format":"SMART_NUMBER","y_axis_bounds":[null,null],"rich_tooltip":true,"tooltipTimeFormat":"smart_date","extra_form_data":{},"dashboards":[]}',
                query_context='{"datasource":{"id":'
                + str(datasets[idx].id)
                + ',"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},"columns":[{"timeGrain":"P1D","columnType":"BASE_AXIS","sqlExpression":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","label":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","expressionType":"SQL"}],"metrics":[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_qll9scn97y_rj89e0zvnw"}],"orderby":[[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_qll9scn97y_rj89e0zvnw"},false]],"annotation_layers":[],"row_limit":10000,"series_columns":[],"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{},"time_offsets":[],"post_processing":[{"operation":"pivot","options":{"index":["'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '"],"columns":[],"aggregates":{"OBS_VALUE":{"operator":"mean"}},"drop_missing_columns":false}},{"operation":"sort","options":{"by":"OBS_VALUE","ascending":false}},{"operation":"flatten"}]}],"form_data":{"datasource":"'
                + str(datasets[idx].id)
                + '__table","viz_type":"echarts_timeseries_bar","x_axis":"'
                + get_locale_column_if_exists(
                    datasets[idx].data, row["xAxisConcept"], locale
                )
                + '","time_grain_sqla":"P1D","x_axis_sort":"OBS_VALUE","x_axis_sort_asc":false,"x_axis_sort_series":"name","x_axis_sort_series_ascending":true,"metrics":[{"expressionType":"SQL","sqlExpression":"OBS_VALUE","column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":false,"label":"OBS_VALUE","optionName":"metric_qll9scn97y_rj89e0zvnw"}],"groupby":[],"adhoc_filters":[],"order_desc":true,"row_limit":10000,"truncate_metric":true,"show_empty_columns":true,"comparison_type":"values","annotation_layers":[],"forecastPeriods":10,"forecastInterval":0.8,"orientation":"vertical","x_axis_title_margin":15,"y_axis_title_margin":15,"y_axis_title_position":"Left","sort_series_type":"sum","color_scheme":"supersetColors","only_total":true,"show_legend":true,"legendType":"scroll","legendOrientation":"top","x_axis_time_format":"smart_date","y_axis_format":"SMART_NUMBER","y_axis_bounds":[null,null],"rich_tooltip":true,"tooltipTimeFormat":"smart_date","extra_form_data":{},"dashboards":['
                + str(dashboard.id)
                + '],"force":false,"result_format":"json","result_type":"full"},"result_format":"json","result_type":"full"}',
                viz_type="echarts_timeseries_bar",
            )

            dashboard.slices.append(chart)
            db.session.add(dashboard)
            db.session.add(chart)

        db.session.commit()


def create_codelist_dataframe(codelist, concept_name):
    codelist_list = []
    for id, code in codelist.items.items():
        item = {"id": id}
        for lang_code, strings in code.name.items():
            item[f"{concept_name}-{lang_code}"] = strings["content"]

        codelist_list.append(item)

    return pd.DataFrame(codelist_list)


def generate_insight_dict(metadata_payload):
    result = {}
    codelists = metadata_payload["Codelists"]
    structure = metadata_payload["DataStructures"]
    if len(structure) != 1:
        raise Exception("One structure expected")
    structure = list(structure.values())[0]
    for id, component in structure.dimension_descriptor.components.items():
        codelist = component.representation.codelist
        if codelist:
            codelist = create_codelist_dataframe(codelist, component.id)
        result[id] = {"name": component.concept_identity.name, "codelist": codelist}
    return result


def generate_final_df_and_concepts_name(data, metadata_payload):
    insight_dict = generate_insight_dict(metadata_payload)
    concepts_names = {}
    for code, component in insight_dict.items():
        concepts_names[code] = component["name"]
        if component["codelist"] is not None:
            data = data.merge(
                component["codelist"],
                left_on=code,
                right_on="id",
                how="inner",
                suffixes=("", f"_{code}"),
            )
            to_drop = [column for column in data.columns if column.startswith(f"id_")]
            data.drop(columns=to_drop, inplace=True, errors="ignore")

    return data, concepts_names


def get_dataflow_id(sdmx_url):
    if "stats.bis.org" in sdmx_url:
        full_id = sdmx_url.split("/data/")[1].split("/")[0]
        if "," in full_id:
            return full_id.split(",")[1]
        return full_id
    elif "sdw-wsrest.ecb.europa.eu" in sdmx_url:
        full_id = sdmx_url.split("/data/")[1].split("/")[0]
        if "," in full_id:
            return full_id.split(",")[1]
        return full_id
    elif "ec.europa.eu" in sdmx_url:
        return sdmx_url.split("/data/")[1][:-1]
    elif "www.ilo.org" in sdmx_url:
        full_id = sdmx_url.split("/data/")[1].split("/")[0]
        if "," in full_id:
            return full_id.split(",")[1]
        return full_id
    raise NotImplementedError("This SDMX provider is not supported yet")


def get_agency_id(sdmx_url):
    if "stats.bis.org" in sdmx_url:
        return "BIS"
    elif "sdw-wsrest.ecb.europa.eu" in sdmx_url:
        return "ECB"
    elif "ec.europa.eu/eurostat" in sdmx_url:
        return "ESTAT"
    elif "www.ilo.org" in sdmx_url:
        return "ILO"
