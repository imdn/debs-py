def parse_triple(triple_str):
    parts = triple_str.split(' ')
    index = 0
    parsed = False
    subject = predicate = object = None
    isValue = False
    
    for part in parts:
        if part == "":
            # More than one space was encountered
            continue
        index += 1
        token = part.strip('<>').split('#')
        if len(token) == 2:
            ns = token[0].split('/')[-1]
            entity = token[1]
            if index == 1:
                subject = (ns,entity)
            elif index == 2:
                isValue = False
                predicate = (ns, entity)
                if entity == "valueLiteral" or entity == "hasNumberOfClusters":
                    # Set flag to interpret next token (object) as literal value
                    isValue = True
            elif index == 3:
                if isValue:
                    # Object is a literal value. Get corresponding object
                    ns = f'{ns}#{entity}' # The datatype of value literal
                    entity = part.split('"')[1]
                object = (ns, entity)
                parsed = True
            else:
                print (f"Something wrong - {line}")
            #print (f"Namespace - {ns}; Entity - '{entity}', {object}")
    #print()
    return (subject, predicate, object)
