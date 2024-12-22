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

    # Returns the required weight of all components in a batch if no filter provided.
    # Otherwise, only for the components found in the filter.
    def get_req_weight(self, filter: list[str] = []) -> float:
        weight = 0
        for component in self.components:
            if len(filter) > 0 and component.name in filter or len(filter) == 0:
                weight += component.corrected_amount
        return weight

    # Returns the actually loaded weight of all components in a batch if no filter provided.
    # Otherwise, only for the components found in the filter.
    def get_actual_weight(self, filter: list[str] = []) -> float:
        weight = 0
        for component in self.components:
            if len(filter) > 0 and component.name in filter or len(filter) == 0:
                weight += component.actually_loaded_amount
        return weight

    # Returns the load mistake of all components in a batch if no filter provided.
    # Otherwise, only for the components found in the filter.
    def get_batch_components_mistake(self, filter: list[str] = None) -> float:
        return sum(self.get_batch_components_mistakes_list(filter))
        # return self.get_actual_weight(filter) - self.get_req_weight(filter)

    def get_batch_components_mistakes_list(self, filter: list[str] = None) -> list[float]:
        return [component.actually_loaded_amount - component.corrected_amount for component in self.components
                if filter is None or component.name in filter]

    name: str
    components: list[ComponentRequirement]
