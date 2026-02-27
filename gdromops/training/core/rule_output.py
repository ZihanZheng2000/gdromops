import os
import re
import numpy as np
from sklearn import tree

def get_ct(ct, s_norm, feature_names, class_names, precision=1):
    
    """
    To extract rules from classification tree, i.e., module application condition
    
    ct: the object of trained model of classification tree
    s_norm: [int] the absolute value of reservoir storage that is used for normalization
    feature_names: [list of str] names of used variables, e.g., [Inflow, Storage, PDSI, DOY]
    class_names: the class names to be classified, i.e., modules 
                    in this case, if not use specified class names, one can simply use ct.classes_.tolist()
    precision: default decimal precision to output each variable
    
    """
    tree_ = ct.tree_
    feature_name = [feature_names[i] if i != tree._tree.TREE_UNDEFINED else "undefined!" for i in tree_.feature]

    paths = []
    path = []
    
    def recurse(node, path, paths):
        if tree_.feature[node] != tree._tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            
            # specifiy decimal precisions for different variables
            if name in ['Inflow', 'Storage']:
                threshold = threshold * s_norm
                precision = 1    # keep 1 decimal
            if name == 'DOY':
                threshold = int(threshold)    # round down DOY to integer
                precision = 1    # randomly assign a value to avoid the error "precision referenced before assignment"
            if name == 'PDSI':
                precision = 2    # keep 3 decimal
            
            p1, p2 = list(path), list(path)
            p1 += [f"({name} <= {np.round(threshold, precision)})"]
            recurse(tree_.children_left[node], p1, paths)
            p2 += [f"({name} > {np.round(threshold, precision)})"]
            recurse(tree_.children_right[node], p2, paths)
        else:
            path += [(tree_.value[node], tree_.n_node_samples[node])]
            paths += [path]
            
    recurse(0, path, paths)
    
    rules = []
    for path in paths:
        rule = "if "
        for p in path[:-1]:
            if rule != "if ":
                rule += " and "
            rule += str(p)
        rule += " then "
        classes = path[-1][0][0]
        l = np.argmax(classes)
#         # this allows to print probability for each rule
#             rule += f"module: {class_names[l]} (proba: {np.round(100.0*classes[l]/np.sum(classes),2)}%)"
        rule += f"module: {class_names[l]}"    
#         # this allows to print number of samples for each rule
#         rule += f" | based on {path[-1][1]:,} samples"
        rules += [rule]
        
    return rules

def get_rt(rt, s_norm, feature_names, class_names, precision=1):

    """
    Extract regression tree rules or module decision logic from DecisionTreeRegressor.
    
    rt: trained DecisionTreeRegressor
    s_norm: storage capacity for denormalizing
    feature_names: list of feature names
    class_names: module labels or None
    precision: number formatting precision
    """
    tree_ = rt.tree_
    feature_name = [feature_names[i] if i != tree._tree.TREE_UNDEFINED else "undefined!" for i in tree_.feature]

    paths = []
    path = []
    
    def recurse(node, path, paths):
        if tree_.feature[node] != tree._tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            if name in ['Inflow', 'Storage']:
                threshold = threshold * s_norm
            p1, p2 = list(path), list(path)
            p1 += [f"({name} <= {np.round(threshold, precision)})"]
            recurse(tree_.children_left[node], p1, paths)
            p2 += [f"({name} > {np.round(threshold, precision)})"]
            recurse(tree_.children_right[node], p2, paths)
        else:
            path += [(tree_.value[node], tree_.n_node_samples[node])]
            paths += [path]
            
    recurse(0, path, paths)

#     # sort by samples count
#     samples_count = [p[-1][1] for p in paths]
#     ii = list(np.argsort(samples_count))
#     paths = [paths[i] for i in reversed(ii)]
    
    rules = []
    for path in paths:
        rule = "if "
        for p in path[:-1]:
            if rule != "if ":
                rule += " and "
            rule += str(p)
        rule += " then "
        if None in class_names or class_names is None:
            rule += "Release: "+str(np.round(path[-1][0][0][0]*s_norm, precision))
        else:
            classes = path[-1][0][0]
            l = np.argmax(classes)
#             rule += f"module: {class_names[l]} (proba: {np.round(100.0*classes[l]/np.sum(classes),2)}%)"
            rule += f"module: {class_names[l]}"    # remove proba
#         rule += f" | based on {path[-1][1]:,} samples"
        rules += [rule]
        
    return rules

def get_rule_string(model_type, params, smax):

    """
    Convert a rule-based model type and its parameters to human-readable string.
    
    model_type: type of rule model (e.g., Constant, Inflow_Linear)
    params: list of parameters
    smax: reservoir storage capacity for denormalizing
    """

    rules = []

    if model_type == 'Constant':
        Release = params[0] * smax
        rules.append(f"Release: {Release:.4f}")

    elif model_type == 'Inflow_Linear':
        k, b = params
        b = b * smax
        rules.append(f"Release = {k:.4f} * Inflow + {b:.4f}")

    elif model_type == 'Inflow_Constant':
        Inflow0, Release1, Release2 = params 
        Inflow0 = Inflow0 * smax
        Release1 = Release1 * smax
        Release2 = Release2 * smax
        rules.append(f"if (Inflow <= {Inflow0:.4f}) then Release: {Release1:.4f}")
        rules.append(f"if (Inflow > {Inflow0:.4f}) then Release: {Release2:.4f}")

    elif model_type == 'Storage_Constant':
        Storage0, Release1, Release2 = params
        Storage0 = Storage0 * smax
        Release1 = Release1 * smax
        Release2 = Release2 * smax
        rules.append(f"if (Storage <= {Storage0:.4f}) then Release: {Release1:.4f}")
        rules.append(f"if (Storage > {Storage0:.4f}) then Release: {Release2:.4f}")

    elif model_type == 'Storage_Hedge':
        Storage0, Release0, k, b = params
        Storage0 = Storage0 * smax
        Release0 = Release0 * smax
        b = b * smax
        rules.append(f"if (Storage < {Storage0:.4f}) then Release: {Release0:.4f}")
        rules.append(f"if (Storage >= {Storage0:.4f}) then Release: max({k:.4f} * Storage + {b:.4f}, 0)")

    elif model_type == 'Joint':
        b, k1, k2 = params 
        b = b * smax
        rules.append(f"Release = {k1:.4f} * Inflow + {k2:.4f} * Storage + {b:.4f}")

    else:
        rules.append("Unknown model type")

    return rules

def get_st(fitting_results, target_id, smax, output_folder):

    """
    Save the rule string of each module to text files.
    
    fitting_results: dict of rule types and parameters for each module
    target_id: reservoir GRAND_ID
    smax: storage capacity
    """

    os.makedirs(output_folder, exist_ok=True)

    for i, (cls_name, result) in enumerate(fitting_results.items()):
        model_type = result['Type']
        params = result['Parameters']
        rule_lines = get_rule_string(model_type, params, smax)

        file_path = os.path.join(output_folder, f"{target_id}_{i}.txt")
        with open(file_path, 'w') as f:
            for rule in rule_lines:
                f.write(f"{rule}\n")



def gen_simp_txt(old_path, new_path):

    """
    Simplify text files
    1. read lines and operate on the lines in following steps
    2. reduce redundant info in two adjacent lines
    3. reduce redundant info within one line
    
    old_path: path to original text file
    new_path: path to generated simplified text file
    """
    
    with open(old_path) as f:
        lines = [line.rstrip('\n') for line in f]

    #################################################
    ## reduce redundant info in two adjacent lines
    ## start from the last condition
    #################################################

    iteration = 0
    print(f'iteration {iteration} complete, size {len(lines)}')
    while True:
        iteration += 1

        old_len = len(lines)
        for i in range(0, len(lines)-1):
            line_1 = lines[i]
            line_2 = lines[i+1]

            if line_1 is None or line_2 is None:
                continue

            # get modules
            mod_1 = line_1[-1]
            mod_2 = line_2[-1]

            # if mod_1 == mod_2, check redundant info
            if mod_1 == mod_2:
                cond_list_1 = re.findall('\(([^)]+)', line_1)    # list of all conditions
                cond_list_2 = re.findall('\(([^)]+)', line_2)    # list of all conditions

                # if length is the same, check redundant info
                if len(cond_list_1) == len(cond_list_2):
                    if cond_list_1[:-1] == cond_list_2[:-1]:    # if only last item is different
                        # compare the last condition
                        var1, operator1, value1 = cond_list_1[-1].split(' ')
                        var2, operator2, value2 = cond_list_2[-1].split(' ')
                        if var1 == var2 and value1 == value2 and operator1 != operator2:    # only operators differ
                            cond_list_1 = cond_list_1[:-1]    # remove the last condition
                            line_1 = 'if (' + ') and ('.join(cond_list_1) + f') then module: {mod_1}'    # write line 1

                            lines[i] = line_1
                            lines[i+1] = None

        # remove duplicates
        # lines = list(dict.fromkeys(lines))

        # remove none
        lines = [x for x in lines if x is not None]

        print(f'iteration {iteration} complete, size {len(lines)}')

        new_len = len(lines)
        if old_len == new_len:
            break
        else:
            old_len = new_len

    ################################################
    ## reduce redundant info within a line
    ################################################

    for i, line in enumerate(lines):
        module = line[-1]
        cond_list = re.findall('\(([^)]+)', line)    # list of all conditions

        # get all variable lowest and highest bounds & store in a dictionary
        cond_bounds = {'Inflow': None, 'Storage': None, 'PDSI': None, 'DOY': None}

        # iterate each variable & get the lowest & highest
        for var in list(cond_bounds.keys()):
            var_low = min([float(item.split('<= ')[1]) for item in cond_list if item.startswith(f'{var} <= ')], default=None)    # get smallest x for (var <= x)
            var_high = max([float(item.split('> ')[1]) for item in cond_list if item.startswith(f'{var} > ')], default=None)    # get biggest x for (inflow > x)

            # add bounds to cond_bounds
            cond_bounds[var] = (var_low, var_high)

        # construct reduced condition line
        cond_list_reduced = []
        for var, (low, high) in cond_bounds.items():
            if low is not None:
                cond_list_reduced.append(f'{var} <= {low}')
            if high is not None:
                cond_list_reduced.append(f'{var} > {high}')

        lines[i] = 'if (' + ') and ('.join(cond_list_reduced) + f') then module: {module}'
    
    with open(new_path, 'w') as f:
        for line in lines:
            f.write(f"{line}\n")

