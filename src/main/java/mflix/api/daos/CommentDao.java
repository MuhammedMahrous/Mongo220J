package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Component
public class CommentDao extends AbstractMFlixDao {

    public static String COMMENT_COLLECTION = "comments";

    private MongoCollection<Comment> commentCollection;

    private CodecRegistry pojoCodecRegistry;

    private final Logger log;

    @Autowired
    public CommentDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());
        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
        this.pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        this.commentCollection =
                db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Returns a Comment object that matches the provided id string.
     *
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        return commentCollection.find(new Document("_id", new ObjectId(id))).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     *
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise
     * returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {
        try {
            String id = comment.getId();
            if (id == null)
                throw new RuntimeException("Id no set");
            commentCollection.insertOne(comment);
        } catch (Exception e) {
            throw new IncorrectDaoOperation("Exception inserting a wrong comment");
        }
        // handle a potential write exception when given a wrong commentId.
        return comment;
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to
     * running the following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     *
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {
        boolean isInserted = false;
        try {

            Comment comment = getComment(commentId);
            if (comment != null && email != null && email.equals(comment.getEmail())) {
                UpdateResult updateResult = commentCollection.updateOne(eq("_id", new ObjectId(commentId)), Updates.combine(Updates.set("text", text), Updates.set("email", email)));
                isInserted = updateResult.getModifiedCount() == 1;
            }
        } catch (MongoException e) {
            isInserted = false;
        }
        return isInserted;
    }

    /**
     * Deletes comment that matches user email and commentId.
     *
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {
        boolean isDeleted = false;
        validateCommentId(commentId);
        Comment comment = getComment(commentId);
        if (comment == null) {
            isDeleted = false;
        } else {
            if (comment.getEmail() != null && comment.getEmail().equals(email)) {
                DeleteResult deleteResult = commentCollection.deleteOne(Filters.eq("_id", new ObjectId(commentId)));
                isDeleted = deleteResult.getDeletedCount() == 1;
            } else {
                isDeleted = false;
            }
        }
        return isDeleted;
    }

    private void validateCommentId(String commentId) {
        if (commentId == null || "".equals(commentId))
            throw new IllegalArgumentException("Comment Id can't be null or empty");
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the
     * `comments` collection and group the users by number of comments. The list is limited to up most
     * 20 commenter.
     *
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {
        List<Critic> mostActive = new ArrayList<>();
        List<Bson> pipeline = Arrays.asList(
                lookup("users", "email", "email", "users"),
                unwind("$users"),
                sortByCount("$users"),
                addFields(new Field("_id", "$_id.email")),
                limit(20));

        AggregateIterable<Critic> aggregate = commentCollection.aggregate(pipeline,Critic.class);
        aggregate.into(mostActive);

        return mostActive;
    }
}
