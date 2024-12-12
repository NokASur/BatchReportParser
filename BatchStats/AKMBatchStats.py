from BatchStats.BasicBatchStats import BasicBatchStats, infinity
from batch import ComponentRequirement


class AKMBatchStats(BasicBatchStats):
    def __init__(self, name: str = "", mistake: float = infinity, weight: float = infinity,
                 components: list[ComponentRequirement] = None):
        super().__init__(name, mistake, weight, components)

    def quality_check(self) -> bool:
        return self.abs_mistake <= 30

    def significance_check(self) -> bool:
        return self.overall_weight > 50

    def is_completed(self) -> bool:
        return True
