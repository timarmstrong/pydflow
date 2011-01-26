import logging

def parse_cmd_string(cmd_string, path_dict):
    
    #tokens: quote delimited strings, all other tokens are separated
    # by spaces. While in quote, \", \' and \\ are escape sequences
    in_quote = None
    tokens = []
    curr_tok = [] # array of characters
    in_token = False  # Can tell if in_token by curr_tok being "", except
                      # in the case where there is a zero-length quoted string
                      # so we keep a separate bool here for that case
    escaped = False
    substitute_path = False
    for c in cmd_string:
        if in_quote:
            if escaped:
                escaped = False
                curr_tok.append(c)
            elif c == in_quote:
                in_quote = None
            elif c == "\\": # escape char
                escaped = True
            else:
                curr_tok.append(c)
        else:
            # not in quote
            if c.isspace(): # end of current token
                if in_token: # if this was the end of a token
                    tokens.append(process_token(curr_tok, path_dict, substitute_path))
                    curr_tok = []
                    in_token = False
                    substitute_path = False
            elif c == "\'" or c == "\"":
                in_quote = c
                in_token = True
            else:
                in_token = True
                if c == "@":
                    substitute_path = True
                curr_tok.append(c)
    if in_quote:
        raise Exception("Unclosed quote %s in command string: %s" % (in_quote, cmd_string))
    if in_token:
        tokens.append(process_token(curr_tok, path_dict, substitute_path))

    logging.debug("Given pathname dict %s and cmdstring %s, result was %s" % (
            repr(path_dict), cmd_string, repr(tokens)))
    return tokens


def process_token(tok, path_dict, substitute_path):
    tok = ''.join(tok) # concatenate list of strings
    if substitute_path:
        # Strip off "@" and look up path
        path = path_dict.get(tok[1:], None)
        if not path:
            raise Exception("No such argument to app as %s, cannot resolve %s, path_dict: %s" % (
                    tok[1:], tok, repr(path_dict)))
        return path
    else:
        return tok
