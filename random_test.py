import joblib

# Example: Saving parameters
parameters = {'coef': [1.0, 2.0, 3.0], 'intercept': 4.0}
joblib.dump(parameters, 'model_parameters.joblib')

# Loading parameters
loaded_parameters = joblib.load('model_parameters.joblib')
print(loaded_parameters)
