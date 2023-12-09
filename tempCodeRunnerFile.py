# Assign surrogate keys to sport dimension
for j in range(len(sport_dim['sport_key'])):
    sport_dim['sport_key'][j] = surrogate_key_list[i]
    i += 1