from random import random


class Tagger:
    def __init__(self, tag):
        super().__init__()
        self.tag = tag
        self.tagged = False


class ProbTagger(Tagger):
    """
    label things by probability
    """
    def __init__(self, tag, proba):
        assert 0 <= proba <= 1, "probability must be between [0, 1]"
        super().__init__(tag)
        self.proba = proba

    def run(self):
        self.tagged = self.proba > random()
        return self.tagged
