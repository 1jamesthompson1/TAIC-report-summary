import engine.Verify.WeightingComparer as WeightingComparer

####################### Compare reasoning #######################

class TestWeightingReasoning:

    def setup_method(self):
        self.comparer = WeightingComparer.WeightingComparer()
        self.theme = {
            "title": "Mechanical Failure",
            "description": "Mechanical failure refers to the malfunction or breakdown of mechanical systems or components, which can lead to loss of control or other hazardous situations. This can occur due to factors such as fatigue cracking, inadequate maintenance, or failure to meet design specifications. Mechanical integrity is critical in high-risk activities to prevent equipment failure and ensure safety.",
        }


    def test_exact_same_reason(self):
        validation_explanation = """
The primary cause of the accident was a mechanical failure within the jet unit steering system, which led to a loss of control and the boat making "heavy contact with the canyon wall" (3.3). The stud-bolts fastening the steering nozzle assembly to the tailpipe of the jet unit broke due to "fatigue cracking caused by insufficient torque being applied to the nuts and therefore inadequate pre-tension in the stud-bolts" (3.8). This mechanical failure was directly responsible for the accident, making it the most significant safety theme.
"""

        engine_explanation = """
The primary cause of the accident was a mechanical failure within the jet unit steering system, which led to a loss of control and the boat making "heavy contact with the canyon wall" (3.3). The stud-bolts fastening the steering nozzle assembly to the tailpipe of the jet unit broke due to "fatigue cracking caused by insufficient torque being applied to the nuts and therefore inadequate pre-tension in the stud-bolts" (3.8). This mechanical failure was directly responsible for the accident, making it the most significant safety theme.
"""

        percentage = self.comparer.compare_weighting_reasoning(self.theme, validation_explanation, engine_explanation)

        assert percentage == 100
    def test_opposite_reason(self):
        validation_explanation = """
The primary cause of the accident was a mechanical failure within the jet unit steering system, which led to a loss of control and the boat making "heavy contact with the canyon wall" (3.3). The stud-bolts fastening the steering nozzle assembly to the tailpipe of the jet unit broke due to "fatigue cracking caused by insufficient torque being applied to the nuts and therefore inadequate pre-tension in the stud-bolts" (3.8). This mechanical failure was directly responsible for the accident, making it the most significant safety theme.
"""

        engine_explanation = """
The primary cause of the accident was not mechanical failure. There was nothing to suggest that the mechanical failure was the cause of the accident. The mechanical failure was not the most significant safety theme.
"""

        percentage = self.comparer.compare_weighting_reasoning(self.theme, validation_explanation, engine_explanation)

        assert percentage == 0
