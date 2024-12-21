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

    def get_abs_mistake_percentage(self) -> float:
        if self.overall_weight == 0:
            return 0
        return self.abs_mistake / self.overall_weight * 100
