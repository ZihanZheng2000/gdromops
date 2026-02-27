import numpy as np

def calculate_nse(true_values, predicted_values):
    mean_true = np.mean(true_values)
    numerator = np.sum((true_values - predicted_values) ** 2)
    denominator = np.sum((true_values - mean_true) ** 2)
    return 1 - (numerator / denominator)

def calculate_pbias(true_values, predicted_values):
    numerator = np.sum(true_values - predicted_values)
    denominator = np.sum(true_values)
    return (numerator / denominator) 

def calculate_kge(true_values, predicted_values):
    mean_true = np.mean(true_values)
    mean_predicted = np.mean(predicted_values)
    std_true = np.std(true_values)
    std_predicted = np.std(predicted_values)
    correlation = np.corrcoef(true_values, predicted_values)[0, 1]

    alpha = std_predicted / std_true
    beta = mean_predicted / mean_true
    kge = 1 - np.sqrt((correlation - 1)**2 + (alpha - 1)**2 + (beta - 1)**2)
    
    return kge

def calculate_lnse(true_values, predicted_values):
    # Apply log transformation to both true and predicted values
    log_true = np.log(true_values + 1e-6)  # Add a small constant to avoid log(0)
    log_predicted = np.log(predicted_values + 1e-6)

    mean_log_true = np.mean(log_true)
    numerator = np.sum((log_true - log_predicted) ** 2)
    denominator = np.sum((log_true - mean_log_true) ** 2)

    lnse = 1 - (numerator / denominator)
    
    return lnse

import numpy as np

def calculate_nrmse(true_values, predicted_values):

    rmse = np.sqrt(np.mean((true_values - predicted_values) ** 2))
    std_observed = np.std(true_values)
    nrmse = rmse / std_observed
    
    return nrmse

def calculate_high_flow_ntrmse(true_values, predicted_values, lambda_value=3):
    # Apply Box-Cox transformation to emphasize high flows
    def box_cox_transform(x, lambda_value):
        if lambda_value == 0:
            return np.log(x + 1e-6)  # Avoid log(0) by adding a small constant
        else:
            return (x ** lambda_value - 1) / lambda_value

    transformed_true = box_cox_transform(true_values, lambda_value)
    transformed_predicted = box_cox_transform(predicted_values, lambda_value)
    sigma_transformed_true = np.std(transformed_true)
    numerator = np.sqrt(np.mean((transformed_true - transformed_predicted) ** 2))
    ntrmse_high = numerator / sigma_transformed_true

    return ntrmse_high

def calculate_low_flow_ntrmse(true_values, predicted_values, lambda_value=0.3):
    # Apply Box-Cox transformation to emphasize low flows
    def box_cox_transform(x, lambda_value):
        if lambda_value == 0:
            return np.log(x + 1e-6)  # Avoid log(0) by adding a small constant
        else:
            return (x ** lambda_value - 1) / lambda_value

    transformed_true = box_cox_transform(true_values, lambda_value)
    transformed_predicted = box_cox_transform(predicted_values, lambda_value)
    sigma_transformed_true = np.std(transformed_true)
    numerator = np.sqrt(np.mean((transformed_true - transformed_predicted) ** 2))
    ntrmse_low = numerator / sigma_transformed_true

    return ntrmse_low

def validate(observed_release, simulated_release):
    """
    Calculate performance metrics for a single pair of observed and simulated release values.

    Parameters:
    observed_release: The observed release values.
    simulated_release: The simulated release values from the model.

    Returns:
    dict: A dictionary containing the calculated NSE, nRMSE, nTRMSE (high and low), KGE, and LNSE metrics.
    """

    # Calculate the various metrics
    release_nse = calculate_nse(observed_release, simulated_release)
    release_nrmse = calculate_nrmse(observed_release, simulated_release)
    release_high = calculate_high_flow_ntrmse(observed_release, simulated_release, lambda_value=3)
    release_low = calculate_low_flow_ntrmse(observed_release, simulated_release, lambda_value=0.3)
    release_kge = calculate_kge(observed_release, simulated_release)
    release_lnse = calculate_lnse(observed_release, simulated_release)

    # Return the calculated metrics as a dictionary
    return {
        'NSE': release_nse,
        'nRMSE': release_nrmse,
        'nTRMSE High': release_high,
        'nTRMSE Low': release_low,
        'KGE': release_kge,
        'LNSE': release_lnse
    }
