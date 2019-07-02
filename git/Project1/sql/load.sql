Insert Into tfidf(song_id, token, score)
Select 
	song_id, 
	L.token,
 	count * LOG((Select Count(song_id) From song)::float/df)
From (
	Select token, Count(song_id) As df
	From token
	Group By token) 
	L
Join token R
On L.token = R.token;

/*
Select 
	song_id, 
	L.token,
	count,
	df,
	(Select Count(song_id) From song) As tot,
 	count * LOG((Select Count(song_id) From song)::float/df) As score
From (
	Select token, Count(song_id) As df
	From token
	Group By token) 
	L
Join token R
On L.token = R.token
Limit 10;*/