from fog_rml.serializers.NTriplesSerializer import NTriplesSerializer


class TurtleSerializer(NTriplesSerializer):
    """
    Serializer that emits Turtle-compatible triple statements.

    Turtle is a superset of N-Triples, so we can reuse the same RDF term
    formatting and line structure for plain triple output.
    """

    pass
