# Connect to Postgres Instance Using Python

This script allows you to connect to your Postgres instance. 

Dependencies are within the `requirements.txt` file.

In order to run the script you need to know the `host` name. If you have created a Postgres instance locally in you Mac,
then `host = localhost`. You need also to pass in the Postgres password, otherwise you cannot query the database. 
In this case I make use of an environment variable in order to not expose the password. If you want to do
the same you have to:
- check if you have already saved something under `PG_PASSWORD` environment variable. 
  From the terminal: `echo $PG_PASSWORD`.
- If it's empty, then save the password within it editing the `.bash_profile` or `.zshrc` file. In my case I edit the 
  latter. From the terminal type: `vim .zshrc`, in the file insert `export PG_PASSWORD=[yourPassword]` somewhere 
  and save the file. You can also use other editors you do not necessarily have to use vim. 
- Execute the `zshrc` file running from the terminal: `source ~/.zshrc`.
- You now have saved a new environment variable. You can print it to terminal using `echo $PG_PASSWORD`.
