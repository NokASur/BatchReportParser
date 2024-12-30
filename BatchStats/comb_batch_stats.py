from BatchStats.basic_batch_stats import BasicBatchStats, infinity
from Batch.batch import ComponentRequirement


class CombBatchStats(BasicBatchStats):
    def __init__(self, name: str = "", mistake: float = infinity, weight: float = infinity,
                 components: list[ComponentRequirement] = None, mistakes: list[float] = None, start_time: str = None,
                 end_time: str = None):
        super().__init__(name, mistake, weight, components, mistakes)
        self.start_time = start_time
        self.end_time = end_time
        self.update_data(start_time=start_time, end_time=end_time)

    def quality_check(self) -> bool:
        return self.get_abs_mistake_percentage() <= 1.5

    def update_data(self, name: str = "", mistake: float = infinity, weight: float = infinity,
                    components: list[ComponentRequirement] = None, mistakes: list[float] = None, start_time: str = None,
                    end_time: str = None) -> None:
        super().update_data(name, mistake, weight, components, mistakes)
        if start_time:
            self.start_time = start_time
        if end_time:
            self.end_time = end_time

    def significance(self) -> bool:
        return self.overall_weight > 50

    def is_completed(self) -> bool:
        return True
