#!/bin/bash
# minimal install for Insilicos Ensemble Cloud Army

command_exists () {
    type "$1" &> /dev/null ;
}

# Make sure only root can run our script
if [ "$(id -u)" != "0" ] ; then
   echo "This script must be run as root - try using sudo" 1>&2
   exit 1
fi

# yum or apt-get?
if command_exists yum ; then INSTALLER='yum' ; fi
if command_exists apt-get ; then INSTALLER='apt-get' ; fi

# do we have python 2.6 or 2.7?
if command_exists python26 ; then PYTHONEXE='python26' ; 
elif command_exists python27 ; then PYTHONEXE='python27' ; 
elif command_exists python ; then PYTHONEXE='python' ; 
fi
if [ "$PYTHONEXE" = "" ]; then $INSTALLER install -y python26 ; fi
if command_exists python26 ; then PYTHONEXE='python26' ;  
elif command_exists python27 ; then PYTHONEXE='python27' ; 
fi
if [ "$PYTHONEXE" = "" ]; then echo "can't find or install Python, quit"; exit 1 ; fi
$PYTHONEXE -c "import sys; exit(1) if sys.version_info < (2,6) else exit(0) ;"
if [ $? -ne 0 ]; then echo "Python is installed but we need 2.6 or newer, quit."; exit 1 ; fi

# get prebuilt pyCrypto if possible
echo "installing pyCrypto for python ssh communication with AWS..."
$INSTALLER install -y $PYTHONEXE-setuptools
$INSTALLER install -y $PYTHONEXE-devel
$INSTALLER install -y $PYTHONEXE-dev
$INSTALLER install -y $PYTHONEXE-crypto

# download ECA and its setup files
if [ "$SUDO_USER" = "" ]; then 
	HOMEDIR=~ 
	SUDO_USER=`whoami` 
else  
	HOMEDIR=/home/$SUDO_USER  
fi
BASEDIR=$HOMEDIR/InsilicosCloudArmy
INSTALLDIR=$BASEDIR/ECA
ECAURL=http://ica.svn.sourceforge.net/svnroot/ica/branches/2.0/ensemble
mkdir -p $INSTALLDIR/src
cd $INSTALLDIR/src
rm -f $INSTALLDIR/src/download_eca.py
wget --tries=10 $ECAURL/src/download_eca.py
$PYTHONEXE download_eca.py $INSTALLDIR $ECAURL/

# point the scripts at the proper python
WHICHPY=`which $PYTHONEXE`
for f in `ls *.py`; do sed -i -e 's#/opt/local/bin/python#'$WHICHPY'#g' $f; done

# install ECA
$PYTHONEXE setup.py install
chmod a+x *.py
chown -R $SUDO_USER $BASEDIR
