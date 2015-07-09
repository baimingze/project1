Insilicos  Ensemble Cloud Army Installation and Release Notes
for posting at https://sourceforge.net/projects/ica/files/

--------
Downloading and Installing Insilicos Ensemble Cloud Army

The "Download" button at https://sourceforge.net/projects/ica/ will provide you with a zip file that contains a windows installer executable and a linux installer bash script.  In both cases, most of the actual installation is done by downloading at install time, to keep these files small.
For windows, extract and run the ECA_Setup_x.x.x.exe file (where x.x.x is the version number)
For linux, extract and run the setup_eca.sh file.


---------
What's new for Insilicos  Ensemble Cloud Army 2.0

Much better support for 64 bit windows and linux (1.0 was fairly win32-centric)
Support for AWS Elastic Map Reduce
Support for AWS Spot Pricing bids
New neural network example
New decision forest with k-fold cross validation example
Switch to using MIT StarCluster AMIs configured at runtime, instead of our own custom AMIs for more robust MPI