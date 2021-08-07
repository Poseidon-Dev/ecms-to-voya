# Voya Data Tranmission from eCMS

This application is designed specifically for eCMS data collection and transmission
to a Voya FTP connection. 

The file is dumped as a csv following Voya's WIN8 file layout

The file is then encrpted PGP using public SSH

The file is then transmitted to Voya's sFTP network. 

All locally stored files are removed

This application runs on a weekly bases and local files are removed after submission. 

## Authors

* **[Johnny Whitworth (@Poseidon-dev)](https://github.com/poseidon-dev)** 

## Support

If you need some help for something, please reach out to me directly or submit an issue and I'll get to it as soon as I can