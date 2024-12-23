from Batch.component_requirement import *
from Batch.batch import Batch


class CombBatch(Batch):
    def __init__(self, name: str, components: list[ComponentRequirement], end_time: str):
        super().__init__(name, components)
        self.end_time = end_time
