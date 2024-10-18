1d
s/^"?([^,"]*),"?([^,"]*)"?$/INSERT INTO url (url) VALUES ('\''\\1'\'');/g