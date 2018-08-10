
:install_library
git clone https://github.com/awslabs/%~1.git
cd %~1
mkdir build
cd build
cmake %* -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=../../install ../ || goto error
msbuild.exe %~1.vcxproj /p:Configuration=Release || goto error
msbuild.exe INSTALL.vcxproj /p:Configuration=Release || goto error
ctest -V || goto error
cd ../..
EXIT /B %ERRORLEVEL%

cd ../
mkdir install

CALL :install_library s2n
CALL :install_library aws-c-common
CALL :install_library aws-c-io

cd aws-c-mqtt
mkdir build
cd build
cmake %* -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX=../../install ../ || goto error
msbuild.exe aws-c-mqtt.vcxproj /p:Configuration=Release || goto error
msbuild.exe tests/aws-c-mqtt-tests.vcxproj /p:Configuration=Release || goto error
ctest -V

goto :EOF

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%