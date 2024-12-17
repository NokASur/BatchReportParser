from BatchStats.BasicBatchStats import BasicBatchStats, infinity
from batch import ComponentRequirement


class LoaderBatchStats(BasicBatchStats):
    def __init__(self, name: str = "", mistake: float = infinity, weight: float = infinity,
                 components: list[ComponentRequirement] = None):
        super().__init__(name, mistake, weight, components)

    def quality_check(self) -> bool:
        return self.get_abs_mistake_percentage() <= 2

    def significance_check(self) -> bool:
        return self.overall_weight > 50

    def is_completed(self) -> bool:
        return True

    def get_abs_mistake_percentage(self) -> float:
        if self.overall_weight == 0:
            return 0
        return self.abs_mistake / self.overall_weight * 100
