APPNAME=$1
TEAMNAME=$2

echo "Replacing <appname> and <teamname> in azure-pipeline.yaml"
sed -i '' "s/<appname>/${APPNAME}/" pipelines/azure-pipeline.yaml
sed -i '' "s/<teamname>/${TEAMNAME}/" pipelines/azure-pipeline.yaml
sed -i '' "s/<serviceconnection-to-github>/github-${TEAMNAME}/" pipelines/azure-pipeline.yaml
