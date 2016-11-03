import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * MyFirstHBase
 * @author Victor Thuillier
 */
public class MyFirstHBase {

    private Configuration conf = null;
    private TableName tableName = TableName.valueOf("vthuillier");
    private Connection connection = null;
    private Table table = null;

    private static final int CHOICE_GET_ONE = 1;
    private static final int CHOICE_GET_ALL = 2;
    private static final int CHOICE_ADD = 3;
    private static final int CHOICE_REMOVE = 4;
    private static final int CHOICE_ADD_FRIEND = 5;
    private static final int CHOICE_REMOVE_FRIEND = 6;
    private static final int CHOICE_EXIT = 7;

    // Database methods

    /**
     * Create a table
     */
    public void createTable(String[] families) throws Exception {
        Admin admin = connection.getAdmin();

        // Check first if the table exists
        if (!admin.tableExists(tableName)) {
            // If not, it is created
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);

            // Each column family is added to the table description
            for (int i = 0; i < families.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(families[i]));
            }

            // The table is finally created
            admin.createTable(tableDesc);
        }
    }

    /**
     * Put (or insert) a row
     */
    public void addRecord(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value) throws Exception {
        // Put object initialized with the rowKey (name of the user)
        Put put = new Put(rowKey);

        // Add to the put operation the corresponding row key, column family, qualifier and value
        put.add(new KeyValue(rowKey, family, qualifier, value));

        // Execute the put operation
        table.put(put);
    }

    /**
     * Delete a row
     */
    public void delRecord(byte[] rowKey) throws IOException {
        // Create the list of elements to delete
        List<Delete> list = new ArrayList<Delete>();

        // Add to this list the element corresponding to rowKey
        Delete del = new Delete(rowKey);
        list.add(del);

        // Execute the delete operation
        table.delete(list);
    }


    // User management methods

    /**
     * Get a user
     */
    public User getUser(String name) throws IOException {
        // Create Get object corresponding to the key (= name)
        Get get = new Get(name.getBytes());
        Result rs = table.get(get);

        // If nothing is found, let's return null
        if (rs.isEmpty()) {
            return null;
        }

        // Create ArrayWritable instance to store the others friends bytes array
        ArrayWritable w = new ArrayWritable(Text.class);
        w.readFields(
            new DataInputStream(
                new ByteArrayInputStream(
                    rs.getValue(Bytes.toBytes("friends"), Bytes.toBytes("others"))
                )
            )
        );

        // Let's convert the writable friends list to a string array
        ArrayList<String> otherFriends = fromWritable(w);

        // The User instance can now be created
        User user = new User(name,
                Bytes.toString(rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))),
                Bytes.toString(rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"))),
                Bytes.toString(rs.getValue(Bytes.toBytes("friends"), Bytes.toBytes("bff"))),
                otherFriends
        );

        return user;
    }

    /**
     * Check whether a user exists or not
     */
    public boolean userExists(String name) throws IOException {
        return getUser(name) != null;
    }

    /**
     * Save a user into the database
     */
    public void saveUser(User user) throws Exception {
        // To save a user, its email, age and bff are saved
        addRecord(Bytes.toBytes(user.getName()), Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(user.getEmail()));
        addRecord(Bytes.toBytes(user.getName()), Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(user.getAge()));
        addRecord(Bytes.toBytes(user.getName()), Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(user.getBff()));

        // Before saving the other friends array, it must be converted into array of bytes
        ArrayList<String> otherFriends = user.getOtherFriends();

        // The other friends string array is converted to a WritableArray, and then to a byte array
        Writable friendsWritable = toWritable(otherFriends);
        byte[] friendsBytes = WritableUtils.toByteArray(friendsWritable);

        // The list 'as bytes) is finally saved
        addRecord(Bytes.toBytes(user.getName()), Bytes.toBytes("friends"), Bytes.toBytes("others"), friendsBytes);
    }

    /**
     * Display a user's information
     */
    public void displayUser(String name) throws IOException{
        // Retrieve the user
        User user = getUser(name);

        // Checks if the user exists
        if (user == null) {
            System.out.println("The user does not exist.");
        }
        // If it exists
        else {
            // Let's print all the information
            System.out.println("\nUser " + user.getName() + ":");
            System.out.println("- Email: " + user.getEmail());
            System.out.println("- Age: " + user.getAge());
            System.out.println("- BFF: " + user.getBff());
            System.out.println("- Other friends: " + String.join(", ", user.getOtherFriends()));
        }
    }


    // ArrayList to ArrayWriter conversion methods

    public Writable toWritable(ArrayList<String> list) {
        Writable[] content = new Writable[list.size()];

        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }

        return new ArrayWritable(Text.class, content);
    }

    public ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);

        for (Writable wrt : writables) {
            list.add(((Text)wrt).toString());
        }

        return list;
    }


    // Menu options methods

    /**
     * Ask a name to display the corresponding user info
     */
    public void processGetOne() throws IOException {
        Scanner sc = new Scanner(System.in);

        System.out.print("What is the name of the person you want to get? ");
        String name = sc.nextLine();

        displayUser(name);
    }

    /**
     * Display every user information
     */
    public void processGetAll() throws IOException {
        // Use Scan to look into the table
        Scan scan = new Scan();

        // Only the row keys are necessary
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner scanner = table.getScanner(scan);

        // For each key, get the corresponding record
        for (Result result : scanner) {
            displayUser(Bytes.toString(result.getRow()));
        }
    }

    /**
     * Add a user to the database
     */
    public void processAdd() throws Exception {
        Scanner sc = new Scanner(System.in);

        System.out.print("Name of the new person: ");
        String name = sc.nextLine();

        System.out.print("Age: ");
        String age = sc.nextLine();

        System.out.print("Email: ");
        String email = sc.nextLine();

        String bff;

        do {
            System.out.print("BFF: ");
            bff = sc.nextLine();

            if ("".equals(bff)) {
                System.out.println("BFF cannot be blank.");
            }
        } while("".equals(bff));

        User user = new User(name, email, age, bff, new ArrayList<String>());
        saveUser(user);
    }

    /**
     * Remove a user from the database
     */
    public void processRemove() throws IOException {
        Scanner sc = new Scanner(System.in);

        System.out.print("Name of the person to remove: ");
        String name = sc.nextLine();

        if (userExists(name)) {
            delRecord(name.getBytes());
            System.out.println("The user has successfully been removed.");
        }
        else {
            System.out.println("The user does not exist.");
        }
    }

    /**
     * Add a friend to a user friends list
     */
    public void processAddFriend() throws Exception {
        Scanner sc = new Scanner(System.in);

        System.out.print("Name of the person to add a friend to: ");
        String name = sc.nextLine();

        System.out.print("Name of the new friend: ");
        String friend = sc.nextLine();

        // Retrieve user
        User user = getUser(name);

        // Checks if the user exists
        if (user == null) {
            System.out.println("The user does not exist.");
        }
        // Check if the friend exists
        else if (!userExists(friend)) {
            System.out.println("The friend is not an existing user.");
        }
        // If the friend is already in the list
        else if (user.hasOtherFriend(friend)) {
            System.out.println("The friend is already in the list.");
        }
        else {
            // The friend is added and saved
            user.addOtherFriend(friend);
            saveUser(user);

            System.out.println("The friend has been added to the list.");
        }
    }

    /**
     * Remove a friend from a user friends list
     */
    public void processRemoveFriend() throws Exception {
        Scanner sc = new Scanner(System.in);

        System.out.print("Name of the person to remove a friend: ");
        String name = sc.nextLine();

        System.out.print("Name of the friend to remove: ");
        String friend = sc.nextLine();

        // Retrieve user
        User user = getUser(name);

        // Check if user exists
        if (user == null) {
            System.out.println("The user does not exist.");
        }
        // If the friend is not in the list
        else if (!user.hasOtherFriend(friend)) {
            System.out.println("The specified friend does not belong to the user's list.");
        }
        else {
            // Remove friend from the list
            user.removeOtherFriend(friend);
            saveUser(user);

            System.out.println("The friend has successfully been removed from the user's list.");
        }
    }

    public static void main(String[] args) {
        try {
            MyFirstHBase myfirsthbase = new MyFirstHBase();

            myfirsthbase.conf = HBaseConfiguration.create();
            myfirsthbase.conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            myfirsthbase.connection = ConnectionFactory.createConnection(myfirsthbase.conf);
            myfirsthbase.table = myfirsthbase.connection.getTable(myfirsthbase.tableName);

            Logger.getRootLogger().setLevel(Level.WARN);

            String[] families = { "info", "friends" };
            myfirsthbase.createTable(families);

            Scanner sc = new Scanner(System.in);
            int choice;

            System.out.println("Welcome to the new amazing and revolutionary social network.\n");

            do {
                System.out.println("\nPlease choose between these options:");
                System.out.println(CHOICE_GET_ONE + ". Get a specific user");
                System.out.println(CHOICE_GET_ALL + ". Get all users");
                System.out.println(CHOICE_ADD + ". Add a user");
                System.out.println(CHOICE_REMOVE + ". Remove a user");
                System.out.println(CHOICE_ADD_FRIEND + ". Add a friend to a user");
                System.out.println(CHOICE_REMOVE_FRIEND + ". Remove a user's friend");
                System.out.println(CHOICE_EXIT + ". Exit");

                System.out.print("\nYour choice: ");

                choice = Integer.parseInt(sc.nextLine());

                switch (choice) {
                    case CHOICE_GET_ONE:
                        myfirsthbase.processGetOne();
                        break;

                    case CHOICE_GET_ALL:
                        myfirsthbase.processGetAll();
                        break;

                    case CHOICE_ADD:
                        myfirsthbase.processAdd();
                        break;

                    case CHOICE_REMOVE:
                        myfirsthbase.processRemove();
                        break;

                    case CHOICE_ADD_FRIEND:
                        myfirsthbase.processAddFriend();
                        break;

                    case CHOICE_REMOVE_FRIEND:
                        myfirsthbase.processRemoveFriend();
                        break;

                    case CHOICE_EXIT:
                        System.out.println("Goodbye! Was great to see you :)");
                        break;

                    default:
                        System.out.println("\nYour choice is invalid.");
                }
            } while (choice != CHOICE_EXIT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}