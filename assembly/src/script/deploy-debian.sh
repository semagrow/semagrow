echo "DEPLOYING DEBIAN" 

DEBIAN_REPOSITORY_ROOT="${distribution.deb.repository}"

cd "target"
FILES=${project.artifactId}*.deb
for f in $FILES
do
    DEBIAN_PACKAGE="`pwd`/$f"
    cd $DEBIAN_REPOSITORY_ROOT
    reprepro remove lucid semagrow
    reprepro includedeb lucid $DEBIAN_PACKAGE
done

echo "DEPLOYED"
