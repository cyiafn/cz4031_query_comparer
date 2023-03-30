import re

diffInQuery = [
    'Removed: c_custkey = o_custkey',
    "Modified: AND r_name = 'ASIA'  => AND r_name = 'nonASIA'",
    'Removed: AND c_acctbal > 10',
    "Added: AND customer.name LIKE 'cheng'"
]

#def explain(diffInQuery, diffInPlan):
def explain():
    explaination = ""
    
    # Define removed/modified/added
    status_regex = re.compile(r'(Removed|Modified|Added):\s*')
    diff_regex = re.compile(r':\s*(.*)')
    # Define regular expressions for each component type
    comparison_regex = re.compile(r"([\w\.]+)\s*(=|!=|<|>|<=|>=|<>|LIKE)\s*('[\w-]+'|[\w\.]+)")
    logical_regex = re.compile(r"\b(ALL|AND|ANY|BETWEEN|EXISTS|IN|LIKE|NOT|OR|SOME)\b")
    constant_regex = re.compile(r"'([^']*)'")
    arithmetic_regex = re.compile(r"(?<!['])\b([\w\.]+)\b\s*([+\-*\/%])\s*(?<!['])\b([\w\.]+)\b(?!['])")
    tuple_attr_regex = re.compile(r"([\w\._]+)(?=[\s]*[=<>])")

    count = 0
    for item in diffInQuery:
        if count == 1:
            explaination += "and with "
        else:
            explaination += "With "
        status_match = status_regex.match(item)
        if status_match:
            status = status_match.group(1)
            if status == 'Added':
                explaination += "an addition of "
            elif status == 'Removed':
                explaination += "the removal of "
            elif status == 'Modified':
                explaination += "modification of "
            diff_match = diff_regex.search(item)
            count = 1
            if diff_match:
                diff = diff_match.group(1)
                #print(f"Status: {status}, Diff: {diff}")
                input_str = diff
        
        # Initialize lists to store each type of expression
        comparisons = []
        logicals = []
        arithmetic_ops = []
        constant_values = []
        tuple_attrs = []

        comparison_matches = comparison_regex.findall(input_str)
        for match in comparison_matches:
            comparisons.append(match)

        logical_matches = logical_regex.findall(input_str)
        for match in logical_matches:
            logicals.append(match)

        arithmetic_matches = arithmetic_regex.findall(input_str)
        for match in arithmetic_matches:
            arithmetic_ops.append(match)

        constant_matches = constant_regex.findall(input_str)
        for match in constant_matches:
            constant_values.append(match)

        tuple_attr_matches = tuple_attr_regex.findall(input_str)
        for match in tuple_attr_matches:
            tuple_attrs.append(match)

        # Print the resulting lists
        print("Comparisons:", comparisons)
        count1 = 0
        if status == 'Modified':
            explaination += "join condition changes from "
            for i in comparisons:
                explaination += " ".join(str(j) for j in i) + " "
                if count1 == 0:
                    explaination += "to "
                count1 = 1
        else:
            explaination += "join condition between "
            for i in comparisons:
                explaination += " ".join(str(j) for j in i) + " "


    #diffInPlan = Selection/Join/Projection
    
    #Types of Join:
        #Types of Nested Loop(NL) Join:
            # Tuple Based NL Join
            # Simple NL Join
            # Block-based NL Join
        #Sort-Based Algorithms:
            #Sort Merge Join
    explaination += ",the query plan uses sort-merge join instead. \nBecause,"
            #Refine Merge Join
        #Hash-Based Algorithms:
            #Grace Hash Join
            #Hybrid Hash Join
        #Index-Based Algorithms:
            #Index-Based Join:
                #Clustered
                #Uncluctered
    explaination = explaination.replace("(", "").replace(")", "").replace("'", "")
    print(explaination)

explain()
