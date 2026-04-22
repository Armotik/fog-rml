from abc import ABC, abstractmethod
from typing import Any
from fog_rml.algebra.Tuple import MappingTuple

class Expression(ABC):
    """
    Represents an algebraic expression phi
    """

    @abstractmethod
    def evaluate(self, mapping: MappingTuple) -> Any:
        """
        Evaluate the expression against a given mapping tuple. (eval(phi, t))
        
        According to the algebraic theory, this method is a total function. 
        It must never raise exceptions due to data inconsistencies, missing 
        attributes, or type mismatches. If the expression cannot be 
        meaningfully evaluated against the provided mapping, it must 
        return the EPSILON constant.
        
        :param mapping: Mapping tuple to evaluate against
        :return: Result of the evaluation
        """
        pass
