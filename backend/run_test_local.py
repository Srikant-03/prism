import traceback
try:
    from tests.test_insights import test_insight_models_instantiation, test_generators_with_empty_profile
    print("Running test_insight_models_instantiation...")
    test_insight_models_instantiation()
    print("Running test_generators_with_empty_profile...")
    test_generators_with_empty_profile()
    print("ALL TESTS PASSED")
except Exception as e:
    print("TEST FAILED")
    traceback.print_exc()
