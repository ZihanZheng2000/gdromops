#%%
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.model_selection import GridSearchCV
from .hmm_DT import GaussianHMM
from .metric import *


def create_gaussian_hmm(inimodel, n_components, **kwargs):
    """Create a local GaussianHMM without patching site-packages."""
    return GaussianHMM(inimodel, n_components=n_components, **kwargs)


def _sanitize_hmm_probabilities(model, n_states):
    """
    Backward-compatible helper for local debugging scripts.
    This follows legacy behavior and only fixes zero-sum transition rows.
    """
    if not hasattr(model, "transmat_"):
        return
    k = int(n_states)
    model.transmat_[np.sum(model.transmat_, axis=1) == 0] = 1 / k

def train_hmdt(train_data, impurity):

    """
    Train a series of HMDT (Hidden Mardov Decision Tree) models with different number of modules (states).

    Parameters:
        train_data: DataFrame with ['Inflow', 'Storage', 'Release']
        impurity: integer controlling min impurity (ITS_min = 10^(-impurity))

    Returns:
        List of trained HMDT models with state counts from 1 to 7
    """

    ITS_min = pow(10, -impurity)
    O = train_data['Release'].values.reshape(-1, 1)
    F = train_data[['Inflow', 'Storage']].values

    treemodel = DecisionTreeRegressor(min_impurity_decrease=ITS_min, random_state=41, max_depth=15, min_samples_leaf=10)
    treemodel.fit(F, O)

    trained_models = []
    for i in range(1, 8):
        try:
            model = create_gaussian_hmm(
                treemodel,
                i,
                relax="all",
                verbose=True,
                n_iter=200,
                trials=10
            )
            model.fit(O, F, lengths=None)
            model = model.best_model
            trained_models.append(model)
        except Exception as e:
            print(f"n_components={i}: {e}")
            continue

    return trained_models

def validate_hmdt(validation_data, trained_model):

    """
    Validate HMDT models on NSE (Nash–Sutcliffe Efficiency).

    Parameters:
        validation_data: DataFrame with ['Inflow', 'Storage', 'Release']
        trained_model: list of trained HMDT models

    Returns:
        List of NSE scores corresponding to each model
    """

    O_val = validation_data['Release'].values.reshape(-1, 1)
    F_val = validation_data[['Inflow', 'Storage']].values

    nse_scores = []
    for model in trained_model:
        try:
            logprob, state_sequence = model.decode(O_val, F_val)
            result = np.zeros(O_val.shape[0])

            for j in range(model.n_components):
                if any(state_sequence == j):
                    result[state_sequence == j] = model.model[j].predict(F_val[state_sequence == j])

            nse = calculate_nse(O_val.ravel(), result)
            nse_scores.append(nse)
        except ValueError as e:
            print(f"Skipping a model due to error: {e}")
            continue
    
    return nse_scores

def validate_hmdt_pbias(validation_data, trained_model):

    """
    Validate HMDT models on PBIAS (Percent Bias).

    Parameters:
        validation_data: DataFrame with ['Inflow', 'Storage', 'Release']
        trained_model: list of trained HMDT models

    Returns:
        List of PBIAS scores corresponding to each model
    """

    O_val = validation_data['Release'].values.reshape(-1, 1)
    F_val = validation_data[['Inflow', 'Storage']].values

    pbias_scores = []
    for model in trained_model:
        try:
            logprob, state_sequence = model.decode(O_val, F_val)
            result = np.zeros(O_val.shape[0])

            for j in range(model.n_components):
                if any(state_sequence == j):
                    result[state_sequence == j] = model.model[j].predict(F_val[state_sequence == j])

            pbias = calculate_pbias(O_val.ravel(), result)
            pbias_scores.append(pbias)
        except ValueError as e:
            print(f"Skipping a model due to error: {e}")
            continue  
    
    return pbias_scores

def train_cart_model(train_data, hmdt_model, best_num_state):

    """
    Train a CART (Classification and Regression Tree) using the HMDT-generated module sequences as labels.

    Parameters:
        train_data: DataFrame with features ['Inflow', 'Storage', 'PDSI', 'DOY']
        hmdt_model: selected HMDT model
        best_num_state: number of modules (states) in HMDT

    Returns:
        Fitted DecisionTreeClassifier (CART model)
    """

    O = train_data['Release'].values.reshape(-1, 1)
    F = train_data[['Inflow', 'Storage']].values

    hmdt_model.transmat_[np.sum(hmdt_model.transmat_, axis=1) == 0] = 1 / best_num_state
    _, state_sequence = hmdt_model.decode(O, F)
    train_data['Module'] = state_sequence

    train_y = train_data['Module'].values
    train_x = train_data[['Inflow', 'Storage', 'PDSI', 'DOY']].values

    parameters = {'max_depth': [4, 5, 6, 8, 10], 'min_samples_split': [5, 10, 15, 20], 'min_samples_leaf': [5, 10, 15, 20]}
    DT0 = DecisionTreeClassifier()
    clf = GridSearchCV(DT0, parameters)
    clf.fit(train_x, train_y)
    print(f"Best score: {clf.best_score_}")

    return clf.best_estimator_

def GDROM_predict(df, hmdt, CT_model):

    """
    Predict reservoir release using a two-stage GDROM: CT (module selection) + HMDT (release generation).

    Parameters:
        df: DataFrame with normalized features and Release
        hmdt: trained HMDT model
        CT_model: trained CART model

    Returns:
        df with added ['Simulated_Release', 'Simulated_Storage'] columns
    """

    initial_storage = df['Storage'].iloc[0]
    pre_R, pre_S = [], []
    temp_storage = initial_storage

    for _, row in df.iterrows():
        inflow, doy, pdsi = row['Inflow'], row['DOY'], row['PDSI']
        CT_input = np.array([inflow, temp_storage, pdsi, doy]).reshape(-1, 4)
        module_id = CT_model.predict(CT_input)[0]

        F = np.array([inflow, temp_storage]).reshape(-1, 2)
        release_pred = hmdt.model[module_id].predict(F)[0]
        temp_storage = max(min(temp_storage + inflow - release_pred, 1), 0)
        release_pred = max(release_pred, 0)

        pre_R.append(release_pred)
        pre_S.append(temp_storage)

    df['Simulated_Release'] = pre_R
    df['Simulated_Storage'] = pre_S

    return df

def GDROM_predict_no_iteration(df, hmdt, CT_model):

    """
    Predict release using current storage without storage iteration.

    Returns:
        df with 'Simulated_Release_no_iteration'
    """

    pre_R = []

    for _, row in df.iterrows():
        inflow, storage, doy, pdsi = row['Inflow'], row['Storage'], row['DOY'], row['PDSI']
        
        CT_input = np.array([inflow, storage, pdsi, doy]).reshape(-1, 4)
        module_id = CT_model.predict(CT_input)[0]

        F = np.array([inflow, storage]).reshape(-1, 2)
        release_pred = hmdt.model[module_id].predict(F)[0]
        release_pred = max(release_pred, 0) 
        
        pre_R.append(release_pred)

    df['Simulated_Release_no_iteration'] = pre_R

    return df

def plot_best_model(train_data, validation_data, its_values=[6, 7, 8]):

    """
    Train HMDT models with various ITS_min and number of modules.
    Plot NSE performance vs. number of modules and its, helping to manually check the combinition.

    Returns:
        trained_models_dict: all trained models
        nse_scores: dict {ITS: list of NSEs}
        pbias_scores: dict {ITS: list of PBIAS}
    """

    nse_scores = {}
    pbias_scores = {}
    trained_models_dict = {}

    # Train models and calculate NSE scores for different ITS values
    for its in its_values:
        models = train_hmdt(train_data, its)
        trained_models_dict[its] = models
        nse_scores[its] = validate_hmdt(validation_data, models)
        pbias_scores[its] = validate_hmdt_pbias(validation_data, models)
    
    # Visualize the NSE scores
    fontsize = 13
    plt.figure(figsize=(10, 5)) 
    for its, scores in nse_scores.items():
        plt.plot(range(1, len(scores) + 1), scores, label=f'$\mathregular{{ITS_{{min}} = 10^{{-{its}}}}}$')
    
    plt.xlabel('Number of modules (K)', fontsize=fontsize, fontweight='black')
    plt.ylabel('NSE', fontsize=fontsize, fontweight='black')
    plt.ylim(0, 1)
    plt.title('NSE Validation', fontsize=fontsize, fontweight='black')
    plt.legend(loc='lower right', bbox_to_anchor=(1, 0))
    plt.tight_layout()
    plt.xticks(np.arange(1, len(scores) + 1, step=1))
    plt.show()

    return trained_models_dict, nse_scores, pbias_scores
