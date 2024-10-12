import great_expectations as gx


def create_expectation(expectation_type, **kwargs):
    """
    A simple wrapper to create Great Expectations expectation objects.
    This function exists to provide a consistent interface and potential for future extensions.

    Args:
        expectation_type (str): The type of expectation to create.
        **kwargs: Arguments to pass to the expectation constructor.

    Returns:
        gx.Expectation: The created expectation object.
    """
    expectation_class = getattr(gx.expectations, expectation_type)
    return expectation_class(**kwargs)


# You can add more utility functions here if needed in the future
