ECS 165A Milestone #1

by: Austin Lew, Keon Sadeghi, Wanzhu Zheng, Aryan Punjani, Alex Amiri

Our goal as a group is to design a database system based on a simplified L-Store from scratch alongside a hybrid transactional analytical processing database. This project is split into three parts, each one being focused on a specific milestone that we are attempting to reach as a group to steadily build and complete our database systems. We aim to take risks with this project in order to test the boundaries to see how complex and fast we can make this databse run.

We start by looking at the query function and look at all the following code blocks that we've developed in order to hit the ground running on our database systems:

- One of the more important tools that we use in the query is the delete function. The goal of this function is to look for a specified RID/key which stands for record identifier. The delete function will search for an RID and then will use both a try and except function to check if the key has been successfully deleted. Try will print nothing if the key was able to get deleted, while except on the other hand would print a statement if the key could not be found or maybe it was locked by another part of the database for recieving access.

- Another important function to look at closely is for insert in a query. The insert function will work to accept any new information or data into the database and to place it in the most relevant location. This typically will work to bring in specified columns that can be easily accessable later. Similarly to delete, it will use RIDs/keys in order to successfuly colect the new information into the database. It will use a if and else statement in order to retrun a string of true or false that will return true if the data was successfully inserted and false if there was an error of some kind placing it or it's incompatible with the dtabase.

- Increment is another useful query function that works to increase the size of our keys or columns in order to increase the efficiency of the database. The RIDs/keys and columns all have seperate calls that makes it easier to specify which portion of the database is trying to be incremented specifically. Just like with the insert function, it will also use true and false in order to test whether or not the keys or columns can be incremented in the setting it is presented in.


Another important part of this database is the index function and how it uses its code in order to work alngside not only the query function but the whole database as a whole.

- 
