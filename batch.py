from support_funcs import *


class ComponentRequirement:
    def __init__(self, name: str, amount: float, corrected_amount: float = -1) -> None:
        self.name = name
        self.amount = amount
        self.corrected_amount = corrected_amount

    def __str__(self) -> str:
        return str(self.name) + " " + str(self.amount) + " " + str(self.corrected_amount)

    def __repr__(self) -> str:
        return self.__str__()

    def __lt__(self, other): return self.name < other.name

    def __eq__(self, other):
        return remove_extra_spaces(self.name.strip()) == remove_extra_spaces(other.name.strip()) and abs(
            self.amount - other.amount) / (
                (self.amount + other.amount) / 2) < 0.05

    name: str
    amount: float
    corrected_amount: float


class Batch:
    def __init__(self, name: str, components: list[ComponentRequirement]) -> None:
        self.name = name
        self.components = components

    def __str__(self) -> str:
        return self.name + "\n" + "\n".join(comp.__str__() for comp in self.components) + "\n"

    def __repr__(self) -> str:
        return self.__str__()

    def __len__(self) -> int:
        return self.components.__len__()

    def __lt__(self, other: "Batch") -> bool:
        return self.__str__() < other.__str__()

    def __eq__(self, other: "Batch") -> bool:
        if self.name == other.name and self.components.__len__() == other.components.__len__():
            for comp, other_comp in zip(sorted(self.components), sorted(other.components)):
                if comp != other_comp:
                    return False
            return True
        return False

    def is_significant(self) -> bool:
        weight = 0
        for component in self.components:
            weight += component.amount
        return weight > 50

    name: str
    components: list[ComponentRequirement]
