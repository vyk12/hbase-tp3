import java.util.ArrayList;

public class User {
    private String name;
    private String email;
    private String age;
    private String bff;
    private ArrayList<String> otherFriends;

    public User(String name, String email, String age, String bff, ArrayList<String> otherFriends) {
        this.name = name;
        this.email = email;
        this.age = age;
        this.bff = bff;
        this.otherFriends = otherFriends;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getBff() {
        return bff;
    }

    public void setBff(String bff) {
        this.bff = bff;
    }

    public ArrayList<String> getOtherFriends() {
        return otherFriends;
    }

    public boolean hasOtherFriend(String friend) {
        return this.otherFriends.contains(friend);
    }

    public void addOtherFriend(String friend) {
        this.otherFriends.add(friend);
    }

    public void removeOtherFriend(String friend) {
        this.otherFriends.remove(friend);
    }
}