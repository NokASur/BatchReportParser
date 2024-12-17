from parsed_excel_data import *
from batch import *
from BatchStats.AKMBatchStats import AKMBatchStats
from BatchStats.BasicBatchStats import BasicBatchStats
from BatchStats.MixerBatchStats import MixerBatchStats
from BatchStats.LoaderBatchStats import LoaderBatchStats
from support_funcs import concatenate_list_values, imprint_meta_values_to_sheet
from queue import Queue

# Affected size of the Excel sheet
field_size_y = 50
field_size_x = 50

mark_translation = {
    "date": 'Дата',
    "plan_batch_count": "Запланированных замесов",
    "executed_batch_count": "Выполненных замесов из плана",
    "executed_batch_count_all": "Всего выполненных замесов",
    "executed_batch_count_all_no_mistake": "Всего выполненных замесов без ошибки",
    "plan_completion": 'Выполнение плана',
    "correct_batches_percent": "Процент замесов без превышения ошибки",
    "load_mark": 'Оценка выгрузки',
    "update_received": 'Обновление принято',  # combination with Batch required
    "extra_award": "Премия",  # import json
    "computer_use_mark": 'Использование ПК',
    "incorrect_batches": "Замесы сверхплана",
    "worker": 'Рабочий',
}

valuable_marks: dict[str:list[str]] = {
    "AKMBatchStats": [
        "date",
        "worker",
        "plan_batch_count",
        "executed_batch_count",
        "executed_batch_count_all",
        "executed_batch_count_all_no_mistake",
        "correct_batches_percent",
        "plan_completion",
        "load_mark",
        "update_received",
        "extra_award",
        "incorrect_batches",
    ],
    "MixerBatchStats": [
        "date",
        "plan_batch_count",
        "executed_batch_count",
        "executed_batch_count_all",
        "executed_batch_count_all_no_mistake",
        "correct_batches_percent",
        "plan_completion",
        "load_mark",
        "extra_award",
        "incorrect_batches",
    ],
    "LoaderBatchStats": [
        "date",
        "plan_batch_count",
        "executed_batch_count",
        "executed_batch_count_all",
        "executed_batch_count_all_no_mistake",
        "correct_batches_percent",
        "plan_completion",
        "load_mark",
        "computer_use_mark",
        "extra_award",
        "incorrect_batches",
    ],
    "BasicBatchStats": [
        "date",
        "plan_batch_count",
        "executed_batch_count",
        "correct_batches_percent",
        "plan_completion",
        "load_mark",
        "computer_use_mark",
        "extra_award",
        "incorrect_batches",
    ],
}


def process_pdf_excel_data(ped: ParsedExcelData, pdf: list[Batch]) -> list[list[str]]:
    processed_data = [["" for _ in range(field_size_x)] for _ in range(field_size_y)]

    marks = generate_marks()
    marks["date"] = ped.date
    marks["worker"] = ped.worker_name
    data_columns = generate_data_columns(ped)

    analise_and_imprint_batches(marks, ped, pdf, processed_data)
    imprint_marks(marks, data_columns, processed_data, ped.type())

    return processed_data


def analise_and_imprint_batches(marks: dict, ped: ParsedExcelData, pdf: list[Batch], processed_data: list[list[str]]):
    cur_y = 0
    completed_batches = 0
    completed_batches_all = 0
    quality_batches = 0
    batch_stats_dict = get_batch_stats_dict(ped)

    batch_type = "BasicBatchStats"

    for batch_stat in ped.batch_stats:

        if batch_type == "BasicBatchStats":
            batch_type = batch_stat.__class__.__name__

        completed_batches_all += 1
        if batch_stat.quality_check():
            quality_batches += 1

    marks["plan_batch_count"] = len(pdf)
    for batch in pdf:
        processed_data[cur_y][0] = batch.name
        affiliated_stats: BasicBatchStats|AKMBatchStats|LoaderBatchStats|MixerBatchStats = get_affiliated_batch_stats(batch_stats_dict, batch.name)

        match batch_type:
            case "AKMBatchStats":
                if affiliated_stats is not None:
                    completed_batches += 1
                    #  ???
                    if affiliated_stats.overall_weight == 0:
                        marks["computer_use_mark"] = "-"

                    processed_data[cur_y][1] = "+"
                    processed_data[cur_y][2] = concatenate_list_values(affiliated_stats.mistakes)
                    quality = affiliated_stats.quality_check()
                    if quality:
                        processed_data[cur_y][3] = "+"
                    else:
                        processed_data[cur_y][3] = "-"
                else:
                    processed_data[cur_y][1] = "-"
                    processed_data[cur_y][2] = "0"
                    processed_data[cur_y][3] = "+"

                cur_y += 1
            case "MixerBatchStats":
                if affiliated_stats is not None:
                    completed_batches += 1
                    #  ???
                    if affiliated_stats.overall_weight == 0:
                        marks["computer_use_mark"] = "-"

                    processed_data[cur_y][1] = "+"
                    processed_data[cur_y][2] = concatenate_list_values(affiliated_stats.mistakes)
                    processed_data[cur_y][3] = str(affiliated_stats.get_abs_mistake_percentage())
                    quality = affiliated_stats.quality_check()
                    if quality:
                        processed_data[cur_y][4] = "+"
                    else:
                        processed_data[cur_y][4] = "-"
                else:
                    processed_data[cur_y][1] = "-"
                    processed_data[cur_y][2] = "0"
                    processed_data[cur_y][3] = "0"
                    processed_data[cur_y][4] = "+"

                cur_y += 1
            case "LoaderBatchStats":
                if affiliated_stats is not None:
                    completed_batches += 1
                    #  ???
                    if affiliated_stats.overall_weight == 0:
                        marks["computer_use_mark"] = "-"

                    processed_data[cur_y][1] = "+"
                    processed_data[cur_y][2] = concatenate_list_values(affiliated_stats.mistakes)
                    processed_data[cur_y][3] = str(affiliated_stats.get_abs_mistake_percentage())
                    quality = affiliated_stats.quality_check()
                    if quality:
                        processed_data[cur_y][4] = "+"
                    else:
                        processed_data[cur_y][4] = "-"
                else:
                    processed_data[cur_y][1] = "-"
                    processed_data[cur_y][2] = "0"
                    processed_data[cur_y][3] = "0"
                    processed_data[cur_y][4] = "+"

                cur_y += 1

    marks["executed_batch_count"] = completed_batches
    marks["executed_batch_count_all"] = completed_batches_all
    marks["executed_batch_count_all_no_mistake"] = quality_batches
    if completed_batches == len(pdf):
        marks["plan_completion"] = "+"

    if completed_batches_all != 0:
        marks["correct_batches_percent"] = quality_batches / completed_batches_all * 100
    else:
        marks["correct_batches_percent"] = 100

    if marks["correct_batches_percent"] >= 70:
        marks["load_mark"] = "+"

    marks["incorrect_batches"] = get_incorrect_batches_list(ped, pdf)
    if len(marks["incorrect_batches"]) > 0:
        marks["update_received"] = '-'


# Returns every completed batch that was not planned
def get_incorrect_batches_list(ped: ParsedExcelData, pdf: list[Batch]) -> list[str]:
    result = []
    pdf_names = {}
    for batch in pdf:
        if batch.name not in pdf_names:
            pdf_names[batch.name] = 1
        else:
            pdf_names[batch.name] += 1

    for batch_stat in ped.batch_stats:
        name = batch_stat.name
        if name not in pdf_names:
            result.append(name)
        elif pdf_names[name] < 1:
            result.append(name)
        else:
            pdf_names[name] -= 1

    return result


def get_batch_stats_dict(ped: ParsedExcelData) -> dict[str, Queue[BasicBatchStats]]:
    res = {}
    for batch_stat in ped.batch_stats:
        if batch_stat.name not in res:
            res[batch_stat.name] = Queue()
        res[batch_stat.name].put(batch_stat)
    return res


# Checks if any given batch_name is present in executed batches from Excel report
def get_affiliated_batch_stats(batch_stats_dict: dict[str, Queue[BasicBatchStats]],
                               batch_name: str) -> BasicBatchStats | None:
    if batch_name in batch_stats_dict:
        if not batch_stats_dict[batch_name].empty():
            return batch_stats_dict[batch_name].get()
    return None


def imprint_marks(marks: dict, data_columns: list[str], processed_data: list[list], batch_type: str):
    x_bias = 2
    for column in data_columns:
        if column == '':
            break
        x_bias += 1

    cur_y = 0
    for relevant_mark_name in valuable_marks[batch_type]:
        mark_name = mark_translation[relevant_mark_name] if mark_translation.__contains__(relevant_mark_name) \
            else relevant_mark_name
        processed_data[cur_y][x_bias] = mark_name
        mark_data = marks[relevant_mark_name]
        if type(mark_data) == list:
            processed_data[cur_y][x_bias + 1: x_bias + 1 + len(mark_data)] = mark_data
        else:
            processed_data[cur_y][x_bias + 1] = mark_data
        cur_y += 1


def generate_marks():
    return {
        "date": '-',
        "worker": '-',
        "plan_batch_count": 0,
        "executed_batch_count": 0,
        "executed_batch_count_all": 0,
        "executed_batch_count_all_no_mistakes": 0,
        "correct_batches_percent": 0,
        "plan_completion": '-',
        "load_mark": '-',
        "update_received": '+',  # combination with Batch required
        "extra_award": 0,  # import json
        "computer_use_mark": '+',
        "incorrect_batches": [],
    }


def generate_data_columns(ped: ParsedExcelData) -> list[str]:
    data_columns = ["" for _ in range(field_size_x)]
    match ped.type():
        case "AKMBatchStats":
            columns = ["Название замеса", "Выполнение", "Ошибка оператора", "Ошибка не превышает 30кг"]
        case "MixerBatchStats":
            columns = ["Название замеса", "Выполнение", "Ошибка оператора", "Ошибка в процентах",
                       "Ошибка не превышает 5%"]
        case "LoaderBatchStats":
            columns = ["Название замеса", "Выполнение", "Ошибка оператора", "Ошибка в процентах",
                       "Ошибка не превышает 2%"]
        case "BasicBatchStats":
            columns = ["Empty stats or incorrect stats type"]
        case _:
            columns = ["Incorrect stats type"]

    data_columns[0:len(columns)] = columns
    return data_columns
