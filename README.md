# Description

This is a tool that can recursively change ACLs inside a S3 bucket.  The change ACL commands are executed in
parallel threads for better performance.

# Building
You can build using the bundled Gradle distribution using JDK 1.7+.  To run, simply execute:

Windows:
```
gradlew.bat shadowJar
```

Mac/Linux:
```
./gradlew shadowJar
```

# Usage
To run, execute as a runnable JAR with Java 1.7+.  You can use either a canned ACL like `public-read` or specify
individual users using the `--read-users` and `--full-control-users` arguments.  When using the individual user
arguments, separate multiple users with spaces.

## Examples
Change all objects in the bucket to be public-read:
```
java -jar build/libs/acl-tool-1.0.jar --endpoint http://10.1.83.51:9020 --access-key jason --secret-key AfffffyXTgQGXnD8y --bucket mybucket --use-smart-client --canned-acl public-read
```

Give the user 'jason2' read access to all objects and 'jason3' full control.

```
java -jar build/libs/acl-tool-1.0.jar --endpoint http://10.1.83.51:9020 --access-key jason --secret-key AfffffyXTgQGXnD8y --bucket mybucket --use-smart-client --read-users jason2 --full-control-users jason3
```

Do the same as above, but only change objects under the 2015-11-03/ prefix:
```
java -jar build/libs/acl-tool-1.0.jar --endpoint http://10.1.83.51:9020 --access-key jason --secret-key AfffffyXTgQGXnD8y --bucket mybucket --use-smart-client --read-users jason2 --full-control-users jason3 --prefix 2015-11-03/
```

