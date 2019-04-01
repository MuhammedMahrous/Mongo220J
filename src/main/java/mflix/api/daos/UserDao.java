package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());

        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        boolean isInserted;
        try {
            Bson filter = Filters.eq("email", user.getEmail());
            User current = usersCollection.find(filter).first();
            if (current == null) {
                usersCollection.insertOne(user);
                isInserted = true;
            } else {
                throw new IncorrectDaoOperation("User Already exists");
            }
        } catch (MongoException e) {
            throw new IncorrectDaoOperation("Couldn't add new user");
        }

        return isInserted;
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        boolean isCreatedOrExists = false;
        try {
//            Bson bson = Filters.eq("jwt", new BsonString(jwt));
            Bson bson = new BsonDocument("jwt", new BsonString(jwt));
            Session session = sessionsCollection.find(bson).first();
            if (session == null) {
                session = new Session();
                session.setUserId(userId);
                session.setJwt(jwt);
                sessionsCollection.insertOne(session);
                isCreatedOrExists = true;

            } else {
                isCreatedOrExists = true;
            }
        } catch (MongoException e) {
            isCreatedOrExists = false;
        }
        return isCreatedOrExists;
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        Bson filter = Filters.eq("email", email);
        User user = usersCollection.find(filter).first();
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        Bson filter = Filters.eq("user_id", userId);
        Session session = sessionsCollection.find(filter).first();
        return session;
    }

    public boolean deleteUserSessions(String userId) {
        boolean isDeleted;
        Bson filter = Filters.eq("user_id", userId);
        DeleteResult deleteResult = sessionsCollection.deleteMany(filter);
        isDeleted = deleteResult.getDeletedCount() >= 1;
        return isDeleted;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        boolean isDeleted = false;
        try {
            Bson filter = Filters.eq("email", email);
            DeleteResult deleteResult = usersCollection.deleteOne(filter);

            if (deleteResult.getDeletedCount() == 1) {
                isDeleted = true;
                deleteUserSessions(email);
            } else {
                isDeleted = false;
            }
        } catch (MongoException e) {
            isDeleted = false;
        }

        return isDeleted;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        boolean isUpdated;

        try {

            if (userPreferences != null) {
                UpdateResult updateResult = usersCollection.updateOne(Filters.eq("email", email), Updates.set("preferences", userPreferences));
                isUpdated = updateResult.getModifiedCount() >= 1;
            } else {
                throw new IncorrectDaoOperation("Couldn't Update Preferences");
            }
        } catch (MongoException e) {
            isUpdated = false;
        }
        return isUpdated;
    }
}
