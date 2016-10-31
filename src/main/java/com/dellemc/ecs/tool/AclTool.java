package com.dellemc.ecs.tool;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.emc.object.util.ConfigUri;
import com.emc.object.util.RestUtil;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cwikj on 10/4/16.
 */
public class AclTool {
    private static final int DEFAULT_THREADS = 32;
    private static final int QUEUE_SIZE = 2000;

    private static final String OPT_CANNED = "canned-acl";
    private static final String OPT_READ_USERS = "read-users";
    private static final String OPT_FULL_CONTROL_USERS = "full-control-users";
    private static final String OPT_BUCKET = "bucket";
    private static final String OPT_PREFIX = "prefix";
    private static final String OPT_ACCESS_KEY = "access-key";
    private static final String OPT_SECRET_KEY = "secret-key";
    private static final String OPT_ENDPOINT = "endpoint";
    private static final String OPT_THREADS = "threads";
    private static final String OPT_DEBUG = "debug";
    private static final String OPT_USE_SMART_CLIENT = "use-smart-client";

    private static final String GROUP_ALL_USERS = "AllUsers";
    private static final String GROUP_AUTHENTICATED_USERS = "AuthenticatedUsers";

    public static void main(String[] args) throws Exception {
        boolean debug = false;
        AclTool aclTool = null;
        try {
            CommandLine line = new DefaultParser().parse(options(), args, true);

            if (line.hasOption("h")) {
                printHelp();
                System.exit(1);
            }

            aclTool = new AclTool();
            aclTool.setEndpoint(line.getOptionValue(OPT_ENDPOINT));
            aclTool.setAccessKey(line.getOptionValue(OPT_ACCESS_KEY));
            aclTool.setSecretKey(line.getOptionValue(OPT_SECRET_KEY));
            if (line.hasOption(OPT_PREFIX)) aclTool.setPrefix(line.getOptionValue(OPT_PREFIX));
            if (line.hasOption(OPT_THREADS)) aclTool.setThreads(Integer.parseInt(line.getOptionValue(OPT_THREADS)));
            aclTool.setBucket(line.getOptionValue(OPT_BUCKET));
            if(line.hasOption(OPT_DEBUG)) debug = true;
            aclTool.setDebug(debug);

            if(line.hasOption(OPT_CANNED)) {
                try {
                    CannedAcl ca = CannedAcl.fromHeaderValue(line.getOptionValue(OPT_CANNED));
                    aclTool.setCannedAcl(ca);
                    if(ca == null) {
                        System.err.println("Invalid canned ACL value: " + line.getOptionValue(OPT_CANNED));
                        System.exit(6);
                    }
                } catch(Exception e) {
                    System.err.println("Error parsing Canned ACL value: " + e.getMessage());
                    System.exit(6);
                }
                if(line.hasOption(OPT_READ_USERS) || line.hasOption(OPT_FULL_CONTROL_USERS)) {
                    System.err.println("Cannot combine canned ACL and other ACL options");
                    printHelp();
                    System.exit(2);
                }
            } else {
                if(!line.hasOption(OPT_READ_USERS) && !line.hasOption(OPT_FULL_CONTROL_USERS)) {
                    System.err.printf("Must specify at least one of --%s, --%s, --%s\n", OPT_READ_USERS,
                            OPT_FULL_CONTROL_USERS, OPT_CANNED);
                    printHelp();
                    System.exit(3);
                }
                AccessControlList acl = new AccessControlList();
                if(line.hasOption(OPT_READ_USERS)) {
                    for(String user : line.getOptionValues(OPT_READ_USERS)) {
                        if (GROUP_ALL_USERS.equals(user)) {
                            acl.addGrants(new Grant(Group.ALL_USERS, Permission.READ));
                        } else if(GROUP_AUTHENTICATED_USERS.equals(user)) {
                            acl.addGrants(new Grant(Group.AUTHENTICATED_USERS, Permission.READ));
                        } else {
                            acl.addGrants(new Grant(new CanonicalUser(user, user), Permission.READ));
                        }
                    }
                }
                if(line.hasOption(OPT_FULL_CONTROL_USERS)) {
                    for(String user : line.getOptionValues(OPT_FULL_CONTROL_USERS)) {
                        if (GROUP_ALL_USERS.equals(user)) {
                            acl.addGrants(new Grant(Group.ALL_USERS, Permission.FULL_CONTROL));
                        } else if (GROUP_AUTHENTICATED_USERS.equals(user)) {
                            acl.addGrants(new Grant(Group.AUTHENTICATED_USERS, Permission.FULL_CONTROL));
                        } else {
                            acl.addGrants(new Grant(new CanonicalUser(user, user), Permission.FULL_CONTROL));
                        }
                    }
                }
                // Owner is required
                acl.setOwner(new CanonicalUser(aclTool.getAccessKey(), aclTool.getAccessKey()));

                aclTool.setAcl(acl);
                if(line.hasOption(OPT_USE_SMART_CLIENT)) {
                    aclTool.setUseSmartClient(true);
                }
            }

            // update the user
            final AtomicBoolean monitorRunning = new AtomicBoolean(true);
            final AclTool tool = aclTool;
            Thread statusThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (monitorRunning.get()) {
                        try {
                            System.out.print("Objects updated: " + tool.getUpdatedObjects() + ", Errors: " +
                                    tool.getErrors().size() +"\r");
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            });
            statusThread.setDaemon(true);
            statusThread.start();

            long startTime = System.currentTimeMillis();
            aclTool.run();
            long duration = System.currentTimeMillis() - startTime;
            double xput = (double) aclTool.getUpdatedObjects().get() / duration * 1000;

            monitorRunning.set(false);
            System.out.println();

            System.out.println("Total Objects updated: " + tool.getUpdatedObjects());
            System.out.println(String.format("Duration: %d secs (%.2f/s)", duration / 1000, xput));

            for (String error : aclTool.getErrors()) {
                System.out.println("Error: " + error);
            }

            System.exit(0);

        } catch (Throwable t) {
            System.out.println("Error: " + t.getMessage());
            if (debug) t.printStackTrace();
            if (aclTool != null) System.out.println("Last key before error: " + aclTool.getLastKey());
            System.exit(-1);
        }
    }

    public void run() {
        // Configure connection
        ConfigUri<S3Config> s3Uri = new ConfigUri<S3Config>(S3Config.class);
        S3Config config = null;
        config = s3Uri.parseUri(endpoint).withIdentity(accessKey).withSecretKey(secretKey);
        config.setSmartClient(useSmartClient);
        if(debug) {
            System.out.println(config.toString());
        }
        S3JerseyClient client = new S3JerseyClient(config);
        executor = new EnhancedThreadPoolExecutor(threads, new LinkedBlockingDeque<Runnable>(QUEUE_SIZE));

        List<Future> futures = new ArrayList<>();
        ListObjectsResult listing = null;
        ListObjectsRequest request = new ListObjectsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing == null) listing = client.listObjects(request);
            else listing = retryListMore(client, listing, 0, 10);

            for (S3Object object : listing.getObjects()) {
                lastKey = object.getKey();
                futures.add(executor.blockingSubmit(new UpdateAclTask(client, bucket, RestUtil.urlDecode(object.getKey()))));
            }

            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2);
            }
        } while (listing.isTruncated());

        handleSingleFutures(futures, futures.size());
    }

    private ListObjectsResult retryListMore(S3Client client, ListObjectsResult previousListing, int current, int max) {
        try {
            return client.listMoreObjects(previousListing);
        } catch(S3Exception e) {
            if(e.getHttpCode() == 404) {
                if(current == max) {
                    throw e;
                } else {
                    return retryListMore(client, previousListing, ++current, max);
                }
            } else {
                throw e;
            }
        }
    }


    protected void handleSingleFutures(List<Future> futures, int num) {
        for (Iterator<Future> i = futures.iterator(); i.hasNext() && num-- > 0; ) {
            Future future = i.next();
            i.remove();
            try {
                future.get();
            } catch (InterruptedException e) {
                errors.add(e.getMessage());
            } catch (ExecutionException e) {
                errors.add(e.getCause().getMessage());
            }
        }
    }

    private static Options options() {
        Options opts = new Options();

        opts.addOption(Option.builder().longOpt(OPT_BUCKET).required().hasArg().argName("bucket")
                .desc("The bucket containing the objects to modify").build());
        opts.addOption(Option.builder().longOpt(OPT_PREFIX).hasArg().argName("prefix")
                .desc("Optional.  Change only the objects under the given prefix.").build());
        opts.addOption(Option.builder().longOpt(OPT_ACCESS_KEY).hasArg().argName("access-key").required()
                .desc("Your S3 access key (username)").build());
        opts.addOption(Option.builder().longOpt(OPT_SECRET_KEY).hasArg().argName("secret-key").required()
                .desc("Your S3 secret key").build());
        opts.addOption(Option.builder().longOpt(OPT_ENDPOINT).hasArg().argName("URL").required()
                .desc("Your S3 endpoint URL, e.g. https://object.ecstestdrive.com or http://10.1.1.51:9020").build());
        opts.addOption(Option.builder().longOpt(OPT_THREADS).hasArg().argName("thread-count")
                .desc("Number of threads to use when changing object ACLs.  Defaults to " + DEFAULT_THREADS).build());
        opts.addOption(Option.builder().longOpt(OPT_CANNED).hasArg().argName("canned-acl")
                .desc("Applies a canned ACL to the objects.  Cannot be used with other ACL options.  Valid values: " +
                        "private | public-read | public-read-write | authenticated-read | bucket-owner-read | " +
                        "bucket-owner-full-control").build());
        opts.addOption(Option.builder().longOpt(OPT_FULL_CONTROL_USERS).hasArg().argName("user, user, ...")
                .desc("Sets a list of users that have full control over the objects.  The special groups " +
                        "AuthenticatedUsers or AllUsers may also be used.").build());
        opts.addOption(Option.builder().longOpt(OPT_READ_USERS).hasArg().argName("user, user, ...")
                .desc("Sets a list of users that have read access to the objects. The special groups " +
                        "AuthenticatedUsers or AllUsers may also be used.").build());
        opts.addOption(Option.builder().longOpt(OPT_DEBUG).desc("Print extra debug information").build());
        opts.addOption(Option.builder().longOpt(OPT_USE_SMART_CLIENT)
                .desc("Enable the smart client (client-side load balancer)").build());

        return opts;
    }

    private static void printHelp() {
        new HelpFormatter().printHelp("java -jar acl-tool-(version).jar [options]", options());
    }

    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucket;
    private String prefix;
    private int threads = DEFAULT_THREADS;
    private EnhancedThreadPoolExecutor executor;
    private List<String> errors = Collections.synchronizedList(new ArrayList<String>());
    private String lastKey;
    private CannedAcl cannedAcl;
    private AccessControlList acl;
    private AtomicLong updatedObjects = new AtomicLong(0);
    private boolean debug = false;
    private boolean useSmartClient = false;


    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }


    public AtomicLong getUpdatedObjects() {
        return updatedObjects;
    }


    public List<String> getErrors() {
        return errors;
    }

    public String getLastKey() {
        return lastKey;
    }

    public CannedAcl getCannedAcl() {
        return cannedAcl;
    }

    public void setCannedAcl(CannedAcl cannedAcl) {
        this.cannedAcl = cannedAcl;
    }

    public AccessControlList getAcl() {
        return acl;
    }

    public void setAcl(AccessControlList acl) {
        this.acl = acl;
    }


    protected class UpdateAclTask implements Runnable {
        private S3Client client;
        private String bucket;
        private String key;

        public UpdateAclTask(S3Client client, String bucket, String key) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
        }

        @Override
        public void run() {
            SetObjectAclRequest req = new SetObjectAclRequest(bucket, key);
            if(getCannedAcl() != null) {
                req.setCannedAcl(getCannedAcl());
            } else {
                req.setAcl(acl);
            }
            client.setObjectAcl(req);
            updatedObjects.incrementAndGet();
        }
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isUseSmartClient() {
        return useSmartClient;
    }

    public void setUseSmartClient(boolean useSmartClient) {
        this.useSmartClient = useSmartClient;
    }
}
