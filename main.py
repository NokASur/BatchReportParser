from openpyxl.utils import get_column_letter

import pandas as pd
import time
import pdfplumber
import json
import os

from Batch.comb_batch import CombBatch
from data_processing import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def append_data_row(data: list, data_rows: list, tech_groups_count: int, good_tech_groups_count: int) -> tuple[
    list[str | int], list, int, int]:
    data_rows.append([data[0], data[3], data[2]])
    tech_groups_count += 1
    good_tech_groups_count += 1 if data[2] == 'Нет' else 0
    data = ['', 0, 'Нет', '']
    return data, data_rows, tech_groups_count, good_tech_groups_count


# Function finds the required amount of batches that are just planned for the day and significant
def parse_pdf_report_for_akm_batches(file_path: str) -> list[Batch]:
    batch_list: list[Batch] = []
    with pdfplumber.open(file_path) as pdf:
        element_table = []

        for page in pdf.pages:
            current_tables = page.extract_tables()
            for table in current_tables:
                for table_element in table:
                    element_table.append(table_element)

        # 1 - batch
        # 2 - possible components
        # 0 - (skip)
        # -----------------
        cur_condition: int = 0
        # -----------------
        name: str = ""
        requirements: list[ComponentRequirement] = []

        for element in element_table:
            if element[0] == 'Замес':
                cur_condition = 1
                if len(requirements) > 0:
                    new_batch = Batch(name, requirements)
                    if new_batch.is_significant():
                        batch_list.append(new_batch)
                    name: str = ""
                    requirements: list[ComponentRequirement] = []
                continue
            elif element[0] == 'Компоненты':
                cur_condition = 2
                continue

            if cur_condition == 1:
                name = element[1]
                cur_condition = 0
            elif cur_condition == 2:
                if len(element) == 3 and element[1] is not None and element[2] != "" and element[1] != "":
                    requirements.append(
                        ComponentRequirement(element[1], float(element[2].replace('.', '').replace(',', '.'))))
            else:
                cur_condition = 0

    if len(requirements) > 0:
        new_batch = Batch(name, requirements)
        if new_batch.is_significant():
            batch_list.append(new_batch)

    return sorted(batch_list)


# Function finds the batches that are actually done during the day
def parse_excel_report_for_akm_batches(file_name: str, worker_type: str) -> list[Batch]:
    file = pd.read_excel(file_name, sheet_name='Загрузка')

    # do not change != to is not, pandas is cooked
    current_data_row_h_offset = file[pd.notna(file.iloc[:, 1])].index[0] + 1

    # offsets
    # ----------------------------------------------------
    batch_name_w_offset = 1
    batch_cmp_name_w_offset = 16
    batch_cmp_weight_req_w_offset = 17
    batch_cmp_weight_req_w_offset_corrected = 20
    batch_cmp_actual_loaded_weight = 21
    # comb_batch
    # ----------------------------------------------------
    batch_time_finish_w_offset = 4
    # ----------------------------------------------------
    batch_list: list[Batch] = []
    name = ""
    end_time = ""
    requirements: list[ComponentRequirement] = []

    while current_data_row_h_offset < file.shape[0]:
        batch_name = (file.iloc[current_data_row_h_offset, batch_name_w_offset])
        if pd.notna(batch_name):
            batch_end_time = file.iloc[current_data_row_h_offset, batch_time_finish_w_offset]
            batch_name = remove_extra_spaces(batch_name.strip())
            component_name = file.iloc[current_data_row_h_offset, batch_cmp_name_w_offset]
            component_weight_req = file.iloc[current_data_row_h_offset, batch_cmp_weight_req_w_offset]
            component_weight_req_corrected = file.iloc[
                current_data_row_h_offset, batch_cmp_weight_req_w_offset_corrected]
            component_actually_loaded_weight = file.iloc[current_data_row_h_offset, batch_cmp_actual_loaded_weight]

            requirements.append(
                ComponentRequirement(
                    component_name,
                    component_weight_req,
                    component_weight_req_corrected,
                    component_actually_loaded_weight
                )
            )

            if batch_name != name:
                name = batch_name
                end_time = batch_end_time
        else:
            if name != "":
                if worker_type == "Комбикорм":
                    new_batch = CombBatch(name, requirements, end_time)
                else:
                    new_batch = Batch(name, requirements)
                if new_batch.is_significant():
                    batch_list.append(new_batch)
                name = ""
                end_time = ""
                requirements: list[ComponentRequirement] = []

        if current_data_row_h_offset == file.shape[0] - 1:
            if worker_type == "Комбикорм":
                new_batch = CombBatch(name, requirements, end_time)
            else:
                new_batch = Batch(name, requirements)
            if new_batch.is_significant():
                batch_list.append(new_batch)

        current_data_row_h_offset += 1

    return batch_list


def parse_excel_report(excel_file: str, worker_type: str, batches: list[Batch | CombBatch]) -> ParsedExcelData:
    file_unload = pd.read_excel(excel_file, sheet_name='Выгрузка')

    date = file_unload.iloc[3, 0][file_unload.iloc[3, 0].find('=') + 2:] if worker_type != "Комбикорм" else ""
    worker_name = file_unload.iloc[2, 0][file_unload.iloc[2, 0].find('=') + 2:]

    # We can also automize offsets search later
    # offsets for unload
    # ----------------------------------------------------
    loader_weight_w_offset = 8
    unloaded_weight_w_offset = 11
    # for loader
    indicator_loader_w_offset = 7
    # for Comb
    start_time_w_offset = 15
    end_time_w_offset = 16
    # ----------------------------------------------------
    # offsets for load (Others)
    # ----------------------------------------------------
    batch_group_name_w_offset = 1
    load_components_w_offset = 16
    req_load_weight_w_offset = 20
    actual_load_weight_w_offset = 21
    # ----------------------------------------------------

    current_data_row_h_offset = 7

    cur_batch_index = 0

    batch_stats = BasicBatchStats()
    match worker_type:
        case "АКМ":
            batch_stats = AKMBatchStats()

        case "Миксер":
            batch_stats = MixerBatchStats()

        case "Погрузчик":
            batch_stats = LoaderBatchStats()

        case "Комбикорм":
            batch_stats = CombBatchStats()
            # somehow the indexes are displaced with this type
            current_data_row_h_offset -= 1

    batch_stats_list = []

    # We do not care about unloads, only about loads here
    if worker_type == "Комбикорм":

        finish_start_batch_times_dict: dict[str, str] = {}

        while current_data_row_h_offset < file_unload.shape[0]:
            start_time = file_unload.iloc[current_data_row_h_offset, start_time_w_offset] \
                if current_data_row_h_offset < file_unload.shape[0] else "-"
            end_time = file_unload.iloc[current_data_row_h_offset, end_time_w_offset] \
                if current_data_row_h_offset < file_unload.shape[0] else "-"
            finish_start_batch_times_dict[end_time] = start_time
            current_data_row_h_offset += 2

        for batch in batches:
            cur_req_weight = batch.get_req_weight()
            mistakes = batch.get_batch_components_mistakes_list()
            actual_name = batch.name
            comps = batch.components

            start_time = ""
            if batch.end_time in finish_start_batch_times_dict:
                start_time = finish_start_batch_times_dict[batch.end_time]

            end_time = batch.end_time

            batch_stats.update_data(
                actual_name,
                weight=cur_req_weight,
                components=comps,
                mistakes=mistakes,
                start_time=start_time,
                end_time=end_time
            )

            batch_stats_list.append(batch_stats)
            batch_stats = CombBatchStats()

    # Parsing through the unload report
    else:
        while current_data_row_h_offset < file_unload.shape[0]:
            if worker_type == "АКМ":
                if not pd.isna(file_unload.iloc[current_data_row_h_offset, unloaded_weight_w_offset]):
                    cur_loader_weight = int(file_unload.iloc[current_data_row_h_offset, loader_weight_w_offset])
                    cur_unloaded_weight = int(file_unload.iloc[current_data_row_h_offset, unloaded_weight_w_offset])
                    mistake = cur_unloaded_weight - cur_loader_weight
                    cur_indexed_loader_weight = int(
                        file_unload.iloc[current_data_row_h_offset, indicator_loader_w_offset])
                    mistake_loader = cur_loader_weight - cur_indexed_loader_weight

                    actual_name = batches[cur_batch_index].name

                    batch_stats.update_data(actual_name, mistake, cur_loader_weight,
                                            batches[cur_batch_index].components, loader_mistakes=[mistake_loader])

                    if current_data_row_h_offset == file_unload.shape[0] - 1:
                        batch_stats_list.append(batch_stats)
                        batch_stats = AKMBatchStats()

                else:
                    batch_stats_list.append(batch_stats)
                    batch_stats = AKMBatchStats()
                    cur_batch_index += 1

            elif worker_type == "Миксер":

                components = [
                    "К/корм СУХ1",
                    "К/корм Сух 2",
                    "К/корм Высокий",
                    "Патока",
                    "Вода",
                ]

                if not pd.isna(file_unload.iloc[current_data_row_h_offset, unloaded_weight_w_offset]):

                    cur_req_weight = batches[cur_batch_index].get_req_weight(components)
                    mistake = batches[cur_batch_index].get_batch_components_mistake(components)
                    actual_name = batches[cur_batch_index].name

                    batch_stats.update_data(actual_name, mistake, cur_req_weight, batches[cur_batch_index].components)

                    if current_data_row_h_offset == file_unload.shape[0] - 1:
                        batch_stats_list.append(batch_stats)
                        batch_stats = MixerBatchStats()
                else:
                    batch_stats_list.append(batch_stats)
                    batch_stats = MixerBatchStats()
                    cur_batch_index += 1

            elif worker_type == "Погрузчик":

                components = [
                    "Солома Пш",
                    "Сенаж яма",
                    "Силос Тукаевский",
                    "Силос",
                    "Смесь мясо+барда",
                    "Сухая Барда",
                    "Рапс.шрот",
                    "Птичья мука",
                ]
                if not pd.isna(file_unload.iloc[current_data_row_h_offset, unloaded_weight_w_offset]):
                    cur_req_weight = batches[cur_batch_index].get_req_weight(components)
                    mistake = batches[cur_batch_index].get_batch_components_mistake(components)
                    actual_name = batches[cur_batch_index].name

                    batch_stats.update_data(actual_name, mistake, cur_req_weight, batches[cur_batch_index].components)

                    if current_data_row_h_offset == file_unload.shape[0] - 1:
                        batch_stats_list.append(batch_stats)
                        batch_stats = LoaderBatchStats()
                else:
                    batch_stats_list.append(batch_stats)
                    batch_stats = LoaderBatchStats()
                    cur_batch_index += 1

            current_data_row_h_offset += 1

    parsed_data2 = ParsedExcelData(batch_stats_list, date, worker_name)

    return parsed_data2


def evaluate_guy(writer, sheet_names: list[str], excel_file: str | None, pdf_file: str | None,
                 worker_type: str) -> None:
    # All parsing
    # --------------------------------------------------------------------------------------
    planned_batch_list: list[Batch] = [] if pdf_file is None else parse_pdf_report_for_akm_batches(pdf_file)
    executed_batch_list: list[Batch] = [] if excel_file is None else parse_excel_report_for_akm_batches(excel_file,
                                                                                                        worker_type)

    excel_parsed_data_list: list[ParsedExcelData] = []

    if worker_type == "АКМ":
        excel_parsed_data_list.append(parse_excel_report(excel_file, worker_type, executed_batch_list))

    elif worker_type == "Миксер/Погрузчик":
        excel_parsed_data_list.append(parse_excel_report(excel_file, "Миксер", executed_batch_list))
        excel_parsed_data_list.append(parse_excel_report(excel_file, "Погрузчик", executed_batch_list))
        sheet_names = ["Миксер", "Погрузчик"]

    elif worker_type == "Комбикорм":
        excel_parsed_data_list.append(parse_excel_report(excel_file, worker_type, executed_batch_list))

    else:
        print("Incorrect worker_type")
        return
    # --------------------------------------------------------------------------------------
    for sheet_name, parsed_data in zip(sheet_names, excel_parsed_data_list):
        processed_data = process_pdf_excel_data(parsed_data, planned_batch_list)

        data_df = pd.DataFrame(data=processed_data)
        data_df.columns = generate_data_columns(parsed_data)
        data_df.to_excel(writer, index=False, sheet_name=sheet_name)

        adjust_excel_cells_length(writer, sheet_name)


if __name__ == '__main__':

    output_file = 'WorkerAnalysisResult.xlsx'
    config_file = 'workers.json'

    if not os.path.exists(config_file):
        print(f"Config file doesn't exist! It is expected to be found here: {config_file}")

    with (open(config_file, 'r', encoding='utf-8') as file):
        config_json = json.load(file)
        standard_sheet_number = 1
        any_data_written = False

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for worker in config_json['workers']:
                worker_name: str = worker['name']
                excelReportFile: str = worker['excelReportFile']
                pdfReportFile: str = worker['pdfReportFile']
                worker_type = worker['type']
                if (excelReportFile is None or os.path.exists(excelReportFile)) and (
                        pdfReportFile is None or os.path.exists(pdfReportFile)):
                    try:
                        evaluate_guy(writer, [worker_name], excelReportFile, pdfReportFile, worker_type)
                    except Exception as e:
                        print("Evaluation for " + worker_name + " failed.")
                        print("Reason: ", e.args[0])
                        continue
                    any_data_written = True
                    print("Worker " + worker_name + " evaluated")
                else:
                    print("Evaluation for " + worker_name + " failed. Report files are not available.")

            if not any_data_written:
                pd.DataFrame([
                    "Ни одной корректной пары репортов не было обнаружено."
                    " Настройте .json файл или измените название файлов."
                ]).to_excel(writer, index=False, header=False, sheet_name='Пустой')

    print(f"Данные успешно сохранены в {output_file}")
    time.sleep(2)
