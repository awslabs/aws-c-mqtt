
cd ../
mkdir install

git clone https://github.com/awslabs/aws-c-common.git
cd aws-c-common
mkdir build
cd build
cmake -G %1 -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=../../install ../ || goto error
msbuild.exe aws-c-common.vcxproj /p:Configuration=Release || goto error
msbuild.exe INSTALL.vcxproj /p:Configuration=Release || goto error
ctest -V || goto error
cd ../..

cd aws-c-mqtt
mkdir build
cd build
cmake -G %1 -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=../../install ../ || goto error
msbuild.exe aws-c-mqtt.vcxproj /p:Configuration=Release || goto error
msbuild.exe tests/aws-c-mqtt-tests.vcxproj /p:Configuration=Release || goto error
ctest -V

goto :EOF

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%