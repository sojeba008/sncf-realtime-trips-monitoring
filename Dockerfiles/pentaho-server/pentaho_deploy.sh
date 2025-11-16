#!/bin/bash
set -e

AUTH_USER=admin
AUTH_PASSWORD=password
NEW_AUTH_PASSWORD="${NEW_AUTH_PASSWORD:-password}"

DWH_DATABASE="${DWH_DATABASE:-infocentre}"
DWH_PORT="${DWH_PORT:-8082}"
DWH_HOST="${DWH_HOST:-localhost}"
DWH_USERNAME="${DWH_USERNAME:-postgres}"
DWH_PASSWORD="${DWH_PASSWORD:-postgres}"

cd /opt/schemaspy
java -jar schemaspy-6.2.4.jar -t pgsql   -dp postgresql-42.2.23.jar -s dwh -db $DWH_DATABASE   -host $DWH_HOST   -port $DWH_PORT   -u $DWH_USERNAME   -p $DWH_PASSWORD  -o ./data_schema
cp -r data_schema /opt/pentaho-server/tomcat/webapps/

/opt/pentaho-server/start-pentaho.sh &
sleep 5



curl -s -o /dev/null -w "%{http_code}\n" --basic -u "$AUTH_USER:$AUTH_PASSWORD" -X PUT "http://localhost:8080/pentaho/plugin/data-access/api/datasource/jdbc/connection/infocentre" \
  -H "Content-Type: application/json;charset=UTF-8" \
  -d "{
  \"changed\": true,
  \"usingConnectionPool\": true,
  \"connectSql\": \"\",
  \"databaseName\": \"${DWH_DATABASE}\",
  \"databasePort\": \"${DWH_PORT}\",
  \"hostname\": \"${DWH_HOST}\",
  \"name\": \"infocentre\",
  \"username\": \"${DWH_USERNAME}\",
  \"password\": \"${DWH_PASSWORD}\",
  \"attributes\": {},
  \"connectionPoolingProperties\": {},
  \"extraOptions\": {},
  \"accessType\": \"NATIVE\",
  \"databaseType\": {
    \"defaultDatabasePort\": 5432,
    \"extraOptionsHelpUrl\": \"\",
    \"name\": \"PostgreSQL\",
    \"shortName\": \"POSTGRESQL\",
    \"supportedAccessTypes\": [
      \"NATIVE\",
      \"JNDI\"
    ]
  }
}"



DASHBOARDS_FILEPATH="/opt/dashboard.zip"
CDE_FILEPATH="/opt/cde.zip"

# Upload du dashboard
echo "Upload du dashboard..."
dashboard_response=$(curl -s -o /dev/null -w "%{http_code}" --basic -u "$AUTH_USER:$AUTH_PASSWORD" \
  -X POST "http://localhost:8080/pentaho/api/repo/files/import" \
  -F "overwriteFile=true" \
  -F "logLevel=WARN" \
  -F "retainOwnership=true" \
  -F "fileNameOverride=$(basename $DASHBOARDS_FILEPATH)" \
  -F "importDir=/public" \
  -F "fileUpload=@${DASHBOARDS_FILEPATH}")

echo "Code retour dashboard : $dashboard_response"

# Upload du CDE
echo "Upload du CDE..."
cde_response=$(curl -s -o /dev/null -w "%{http_code}" --basic -u "$AUTH_USER:$AUTH_PASSWORD" \
  -X POST "http://localhost:8080/pentaho/api/repo/files/import" \
  -F "overwriteFile=true" \
  -F "logLevel=WARN" \
  -F "retainOwnership=true" \
  -F "fileNameOverride=$(basename $CDE_FILEPATH)" \
  -F "importDir=/public" \
  -F "fileUpload=@${CDE_FILEPATH}")

echo "Code retour CDE : $cde_response"


# --- Assignation des rôles ---
echo "Assignation des rôles pour l'utilisateur $AUTH_USER..."
roles_response=$(curl --retry 1 -s -o /dev/null -w "%{http_code}" --basic -u "$AUTH_USER:$AUTH_PASSWORD" \
  -X PUT "http://localhost:8080/pentaho/api/userroledao/roleAssignments" \
  -H "Content-Type: application/xml" \
  -d '<systemRolesMap>
        <assignments>
            <roleName>Anonymous</roleName>
            <logicalRoles>org.pentaho.repository.read</logicalRoles>
            <logicalRoles>org.pentaho.security.publish</logicalRoles>
            <logicalRoles>org.pentaho.repository.create</logicalRoles>
        </assignments>
    </systemRolesMap>')
echo "Code retour assignation des rôles : $roles_response"

ANONYMOUS_USER="anonymous"
ANONYMOUS_PASSWORD="anonymous"
BODY_XML="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<user>
  <userName>${ANONYMOUS_USER}</userName>
  <password>${ANONYMOUS_PASSWORD}</password>
</user>"

response=$(curl -s -o /dev/null -w "%{http_code}" \
  --basic -u "${AUTH_USER}:${AUTH_PASSWORD}" \
  -X PUT "http://localhost:8080/pentaho/api/userroledao/createUser" \
  -H "Accept: application/xml" \
  -H "Content-Type: application/xml" \
  -d "${BODY_XML}")

echo "Code retour création utilisateur : $response"

# --- Mise à jour du mot de passe ---
echo "Mise à jour du mot de passe pour $AUTH_USER..."
password_response=$(curl --retry 1 -s -o /dev/null -w "%{http_code}" --basic -u "$AUTH_USER:$AUTH_PASSWORD" \
  -X PUT "http://localhost:8080/pentaho/api/userroledao/updatePassword" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d "{ \"userName\" : \"$AUTH_USER\", \"$AUTH_PASSWORD\" : \"$NEW_AUTH_PASSWORD\", \"administratorPassword\" : \"$AUTH_PASSWORD\"}")

echo "Code retour mise à jour mot de passe : $password_response"

tail -f /opt/pentaho-server/tomcat/logs/catalina.out