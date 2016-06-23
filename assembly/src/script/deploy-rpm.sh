#!/bin/sh

echo "SIGNING PACKAGE"
src/script/rpm-sign.exp target/*.rpm

echo "DEPLOYING RPM"
RPM_REPOSITORY_ROOT="/var/www/rpm"
cp target/*.rpm $RPM_REPOSITORY_ROOT
createrepo $RPM_REPOSITORY_ROOT

