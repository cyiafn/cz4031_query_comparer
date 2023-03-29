import re

#def explain(diffInQuery, diffInPlan):
def explain():
    explaination = ""

    explaination += "With an additional of "
    #diffInQuery[0] = Additional/Removal/Modify
    #diffInQuery[1] = String of the changes

    # if diffInQuery[0] == 'additional':
    #     explaination += "an addition of"
    # elif diffInQuery[0] == 'removal':
    #     explaination += "a removal of"
    # elif diffInQuery[0] == 'modify':
    #     explaination += "modification of"

    # Define regular expressions for each component type
    #Comparison Operators(=, <, >, !=, <=, >=, <>)
    comparison_regex = re.compile(r"([\w\.]+)\s*(=|!=|<|>|<=|>=|<>)\s*('[\w-]+'|[\w\.]+)")
    #Logical Operators(ALL, AND, ANY, BETWEEN, EXISTS, IN, LIKE, NOT, OR, SOME)
    logical_regex = re.compile(r"\b(ALL|AND|ANY|BETWEEN|EXISTS|IN|LIKE|NOT|OR|SOME)\b")
    #Constant Value
    constant_regex = re.compile(r"'([^']*)'")
    #Arithmatic Operators(+, -, *, /, %)
    arithmetic_regex = re.compile(r"(?<!['])\b([\w\.]+)\b\s*([+\-*\/%])\s*(?<!['])\b([\w\.]+)\b(?!['])")
    # #Bitwise Operators(&, |, ^)
    # bitwise_regex = re.compile(r"([\w\.]+)\s*(&|\||\^)\s*([\w\.]+)")
    # #Compound Operators(+=, -=, *=, /=, %=, &=, ^-=, |*=)
    # compound_regex = re.compile(r"([\w\.]+)\s*(\+=|-=|\*=|\/=|%=|&=|\^=|\|=)\s*([\w\.]+)")
    #Tuple Attribute References
    tuple_attr_regex = re.compile(r"([\w\._]+)(?=[\s]*[=<>])")


    # Define the input string
    #input_str = diffInQuery[1]
    #input_str = "AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01'"
    input_str = "C.c_custkey = O.o_custkey AND C.c_name LIKE 'cheng'"

    # Initialize lists to store each type of expression
    comparisons = []
    logicals = []
    arithmetic_ops = []
    # bitwise = []
    # compound = []
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

    # bitwise_matches = bitwise_regex.findall(input_str)
    # for match in bitwise_matches:
    #     bitwise.append(match)
    
    # compound_matches = compound_regex.findall(input_str)
    # for match in compound_matches:
    #     compound.append(match)

    constant_matches = constant_regex.findall(input_str)
    for match in constant_matches:
        constant_values.append(match)

    tuple_attr_matches = tuple_attr_regex.findall(input_str)
    for match in tuple_attr_matches:
        tuple_attrs.append(match)

    # Print the resulting lists
    count = 0
    if comparisons:
        print("Comparisons:", comparisons)
        explaination += "comparisons between "
        for i in comparisons:
            explaination += str(i) + " "
        count = 1
    if logicals:
        print("Logicals:", logicals)
        if count == 1:
            explaination += "and "
        explaination += "logical operators such as: "
        for i in logicals:
            explaination += str(i) + " "
        count = 1
    if arithmetic_ops:
        print("Arithmetic Operations:", arithmetic_ops)
    if constant_values:
        print("Constant Values:", constant_values)
    if tuple_attrs:
        print("Tuple Attrs:", tuple_attrs)

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
    print(explaination)

explain()
