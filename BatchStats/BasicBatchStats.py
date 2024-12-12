from batch import ComponentRequirement
infinity: float = 100000000000000

# Basic maternal class for all the BatchStats classes
# Not intended for use on its own

class BasicBatchStats:

    def __init__(self, name: str = "", mistake: float = infinity, weight: float = infinity,
                 components: ComponentRequirement = None):
        self.name: str
        self.weights: list[float] = []
        self.overall_weight: float = 0.0
        self.mistakes: list[float] = []
        self.abs_mistake: float = 0.0
        self.components: list[ComponentRequirement] = []

        self.update_data(name, mistake, weight, components)

    def update_data(self, name: str = "", mistake: float = infinity, weight: float = infinity,
                    components: list[ComponentRequirement] = None):

        self.name = name

        if mistake != infinity:
            self.mistakes.append(mistake)
            self.abs_mistake += abs(mistake)

        if weight != infinity:
            self.weights.append(weight)
            self.overall_weight += weight

        if components is not None:
            self.components.extend(components)

    def quality_check(self) -> bool:
        return False

    def significance_check(self) -> bool:
        return False

    def is_completed(self) -> bool:
        return False

    def __str__(self):
        return (
            f"Names: {self.names}\n"
            f"Total Weight: {self.overall_weight}\n"
            f"Mistakes: {self.mistakes}\n"
            f"Absolute Mistake: {self.abs_mistake}\n"
        )

    def __repr__(self):
        return self.name

    def __lt__(self, other) -> bool:
        return self.name < other.name

# def batch_stats_to_parsed_excel_data(batch_stats: list[BasicBatchStats], date: str = "Undefined",
#                                      worker_name: str = "Undefined", data_columns=None) -> ParsedExcelData:
#     if data_columns is None:
#         data_columns = []
#
#     batch_count = 0
#     correct_batch_count = 0
#
#     for batch_stat in batch_stats:
#         if batch_stat.quality_check():
#
#     return ParsedExcelData()
