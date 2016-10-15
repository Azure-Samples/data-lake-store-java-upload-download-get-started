package com.contoso.sample;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

public class UploadDownloadApp {

    // This needs to be filled in for the app to work
    private static String accountFQDN = "FILL-IN-HERE";  // full account FQDN, not just the account name
    private static String clientId = "FILL-IN-HERE";
    private static String authTokenEndpoint = "FILL-IN-HERE";
    private static String clientKey = "FILL-IN-HERE";

    public static void main(String[] args) {
        try {
            // Obtain OAuth2 token and use token to create client object
            AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(authTokenEndpoint, clientId, clientKey);
            ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, token);

            // create directory
            client.createDirectory("/a/b/w");

            // create file and write some content
            String filename = "/a/b/c.txt";
            OutputStream stream = client.createFile(filename, IfExists.OVERWRITE  );
            PrintStream out = new PrintStream(stream);
            for (int i = 1; i <= 10; i++) {
                out.println("This is line #" + i);
                out.format("This is the same line (%d), but using formatted output. %n", i);
            }
            out.close();

            // set file permission
            client.setPermission(filename, "744");

            // append to file
            stream = client.getAppendStream(filename);
            stream.write(getSampleContent());
            stream.close();

            // Read File
            InputStream in = client.getReadStream(filename);
            byte[] b = new byte[64000];
            while (in.read(b) != -1) {
                System.out.write(b);
            }
            in.close();

            // get file metadata
            DirectoryEntry ent = client.getDirectoryEntry(filename);
            printDirectoryInfo(ent);

            // create another file - this time using a byte array
            stream = client.createFile("/a/b/d.txt", IfExists.OVERWRITE);
            byte[] buf = getSampleContent();
            stream.write(buf);
            stream.close();

            // concatenate the two files into one
            List<String> fileList = Arrays.asList("/a/b/c.txt", "/a/b/d.txt");
            client.concatenateFiles("/a/b/f.txt", fileList);

            //rename the file
            client.rename("/a/b/f.txt", "/a/b/g.txt");

            // list directory contents
            List<DirectoryEntry> list = client.enumerateDirectory("/a/b", 2000);
            System.out.println("Directory listing for directory /a/b:");
            for (DirectoryEntry entry : list) {
                printDirectoryInfo(entry);
            }

            // delete directory along with all the subdirectories and files in it
            client.deleteRecursive("/a");

        } catch (ADLException ex) {
            printExceptionDetails(ex);
        } catch (Exception ex) {
            System.out.format(" Exception: %s%n Message: %s%n", ex.getClass().getName(), ex.getMessage());
        }
    }

    private static void printExceptionDetails(ADLException ex) {
        System.out.println("ADLException:");
        System.out.format("  Message: %s%n", ex.getMessage());
        System.out.format("  HTTP Response code: %s%n", ex.httpResponseCode);
        System.out.format("  Remote Exception Name: %s%n", ex.remoteExceptionName);
        System.out.format("  Remote Exception Message: %s%n", ex.remoteExceptionMessage);
        System.out.format("  Server Request ID: %s%n", ex.requestId);
        System.out.println();
    }

    private static void printDirectoryInfo(DirectoryEntry ent) {
        System.out.format("Name: %s%n", ent.name);
        System.out.format("FullName: %s%n", ent.fullName);
        System.out.format("Length: %d%n", ent.length);
        System.out.format("AclType: %s%n", ent.type.toString());
        System.out.format("Group: %s%n", ent.group);
        System.out.format("User: %s%n", ent.user);
        System.out.format("Permission: %s%n", ent.permission);
        System.out.format("mtime: %s%n", ent.lastModifiedTime.toString());
        System.out.format("atime: %s%n", ent.lastAccessTime.toString());
        System.out.println();
    }

    private static byte[] getSampleContent() {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(s);
        out.println("This is a line");
        out.println("This is another line");
        out.println("This is yet another line");
        out.println("This is yet yet another line");
        out.println("This is yet yet yet another line");
        out.println("... and so on, ad infinitum");
        out.println();
        out.close();
        return s.toByteArray();
    }
}
