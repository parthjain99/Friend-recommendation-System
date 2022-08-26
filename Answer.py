import itertools
from pyspark import SparkContext
#Reading the file and then storing them in variable as resilient distributed dataset
sc = SparkContext.getOrCreate()
people = sc.textFile('/content/soc-LiveJournal1Adj.txt')

#We get a line from the resilient distributed dataset and then we split the data into id and a list 
#then we use the map function to convert the string input from the friends of the user list and convert them into integer value 
def map_pairs(line):
    sp = line.split()
    id = int(sp[0])
    if len(sp) == 1:
        return id, []
    else:
        friendsof_people = list(map(lambda x: int(x), sp[1].split(',')))
    return id, friendsof_people

#We are getting the input as id and the friends list and we use the itertools functions to return pair combinations 
#of the freinds list and map them with count 0 if it is id-friend pair and 1 if it is friend to friend pair of a given id
#we also order the pair such way that smaller number is first and larger number is second
def people_connected_friends(friendships):
    id = friendships[0]
    friends = friendships[1]
    connections = []
    for user in friends:
        key = (id, user)
        if id > user:
            key = (user, id)
        connections.append((key, 0))
    for pairs in itertools.combinations(friends, 2):
        friend1 = pairs[0]
        friend2 = pairs[1]
        key = (friend1, friend2)
        if friend1 > friend2:
            key = (friend2, friend1)
        connections.append((key, 1))
    return connections

#Here we are just the input from ((friend_recomendation1,friend_recomendation2),mutualfriend_count) map it to 
#(friend_recomendation1,(friend_recomendation2,mutualfriend_count))
def friend_recommendation_pairs(mutual):
    pair = mutual[0]
    count = mutual[1]
    friend1 = pair[0]
    friend2 = pair[1]
    recomendation1 = (friend1, (friend2, count))
    recomendation2 = (friend2, (friend1, count))
    return [recomendation1, recomendation2]

#We then sort the recomendations in ascending values for a user and descending order of count for that user
def sort_recommendations(recomendations):
    recomendations.sort(key = lambda x: (-int(x[1]), int(x[0])))
    return list(map(lambda x: x[0], recomendations))[:10]

#map the text data in list of list of form
friendship_pairs = people.map(map_pairs)

#Here we map the friend list of all the users to the each of the friends with the key in this format((user,friend),key)
friend_connections = friendship_pairs.flatMap(people_connected_friends)

# finds the number of mutual friends between users who are not already friends and filter out all the user that are already friends
connections = friend_connections.groupByKey().filter(lambda pair: 0 not in pair[1]).map(lambda pair:(pair[0], sum(pair[1])))
# here we get the output in this format  ((friend_pair), sum of mutual friends the friend_pair has in common)
connections.cache()

# get pairs of recommended friends in this format  (user_id, (friend_id, mutual friend count between user_id and friend_id))
recommendations = connections.flatMap(friend_recommendation_pairs)
friend_recommendations = recommendations.groupByKey().map(lambda mf: (mf[0], sort_recommendations(list(mf[1]))))
#from the given list of of people we get the list of top 10 recomended friends in this format (user_id, [list of 10 recommened friends])
user_ids_recs = friend_recommendations.filter(lambda recs: recs[0] in [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]).sortByKey()
user_ids_recs.take(10)

