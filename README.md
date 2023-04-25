# TAIC-report-summary

This is a simple script that will download all the reports found on the TAIC [website](https://www.taic.org.nz/inquiries?order=field_publication_date_value&sort=desc&keyword=&date_filter%5Bmin%5D%5Bdate%5D=&date_filter%5Bmax%5D%5Bdate%5D=&publication_date%5Bmin%5D%5Bdate%5D=&publication_date%5Bmax%5D%5Bdate%5D=&status%5B0%5D=12)

It then extracts the text and preforms a simple extractive summary of each of the pdfs.

There are some problems with it not handling encoding issues but other than that it all sort of works.

You will need to install some packages to get aflot.

Once that is done just try to run the main script.

## Setting up

This will help you setup the repo from a complete scratch

You need to open up a powershell by pressing windwos key then typing "powershell" then clicking on Windows Powershell.

Start with installing scoop
```
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
irm get.scoop.sh | iex
```

Now you need to install git
```
scoop install git
```

Make sure you are in the right place with
```
cd ~
```

Lastly you need to clone the repo:
```
git clone https://github.com/1jamesthompson1/TAIC-report-summary.git
```

However it might also be worth setting up a github account and then your git for good measure.
Instructions to follow
