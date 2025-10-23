package socialmedia

func GetUsersSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		);
	`
}

func GetPostsSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS posts (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
	`
}

func GetFollowsSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS follows (
			follower_id VARCHAR(255) NOT NULL,
			followee_id VARCHAR(255) NOT NULL,
			PRIMARY KEY (follower_id, followee_id)
		);
	`
}

func GetTimelinesSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS timelines (
			user_id VARCHAR(255) PRIMARY KEY,
			post_ids TEXT[] NOT NULL
		);
	`
}

/*
MongoDB document structure:

users: {
  _id: <string>,
  name: <string>
}

posts: {
  _id: <string>,
  user_id: <string>,
  content: <string>,
  created_at: <date>
}

follows: {
  follower_id: <string>,
  followee_id: <string>
}

timelines: {
  user_id: <string>,
  post_ids: [<string>]
}

*/
