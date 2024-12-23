from support_funcs import *


class ComponentRequirement:
    def __init__(self, name: str, amount: float, corrected_amount: float = -1,
                 actually_loaded_amount: float = -1) -> None:
        self.name = name
        self.amount = amount
        self.corrected_amount = corrected_amount
        self.actually_loaded_amount = actually_loaded_amount

    def __str__(self) -> str:
        return str(self.name) + " " + str(self.amount) + " " + str(self.corrected_amount)

    def __repr__(self) -> str:
        return self.__str__()

    def __lt__(self, other): return self.name < other.name

    def __eq__(self, other):
        return remove_extra_spaces(self.name.strip()) == remove_extra_spaces(other.name.strip()) and abs(
            self.amount - other.amount) / (
                (self.amount + other.amount) / 2) < 0.05

    def get_absolute_component_mistake(self) -> float:
        return self.actually_loaded_amount - self.corrected_amount

    def get_mistake_percentage(self) -> float:
        if self.corrected_amount == 0:
            return 0
        return self.get_absolute_component_mistake() / self.corrected_amount * 100

    name: str
    amount: float
    corrected_amount: float
    actually_loaded_amount: float
