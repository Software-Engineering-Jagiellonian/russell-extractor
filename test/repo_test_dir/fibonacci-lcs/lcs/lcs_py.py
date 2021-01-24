"""
Given a list of fingerprints for "windows" in size of t-1
return a list of fingerprints for "windows" in size t using
Horner's rule
"""
def extend_fingerprints(text, fingers, t, basis=2 ** 8, r=2 ** 17 - 1):
    for i in range(len(fingers) - 1):
        fingers[i] = (fingers[i] * basis + ord(text[t+i-1])) % r
    fingers.pop()
    return None

"""
Given a list of fingerprints and table_size , 
return a hashtable in size of table_size which is a list that it's j element
is an ordered list of indexes i that fingers[i] = j
"""

def make_hashtable(fingers, table_size):
    hash_table = []
    for j in range(table_size):
        hash_table.append([i for i in range(len(fingers)) if fingers[i] == j])
    return hash_table

"""
Given two strings and their fingerprints, t , basis r
return common subsequence of the two strings in length t if such exist , else None
"""

def find_match(text1, text2, fingers1, fingers2, t, r):
    hash_table = make_hashtable(fingers1, r)
    result = None
    for i in range(len(fingers2)):
        for index in hash_table[fingers2[i]]:
            if text1[index:index+t] == text2[i: i+t]:
                result = text1[index: index+t]
    return result

"""
Finding the lcs of two strings using previous methods 
"""
def find_longest(text1, text2, basis=2 ** 8, r=2 ** 17 - 1):
    match = ''
    l = 0  # initial "window" size
    # fingerprints of "windows" of size 0 - all are 0
    fingers1 = [0] * (len(text1) + 1)
    fingers2 = [0] * (len(text2) + 1)

    longest = None

    while match is not None:  # increasing length of common subsequences
        l += 1
        extend_fingerprints(text1, fingers1, l, basis, r)
        extend_fingerprints(text2, fingers2, l, basis, r)
        match = find_match(text1, text2, fingers1, fingers2, l, r)
        if match is not None:
            longest = match
    return longest

