package surge.annotations;

import akka.annotation.ApiMayChange;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Experimental annotation used to indicate that a particular class or method should be considered experimental to denote
 * that it can change in backwards incompatible ways without a major version bump.
 */
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ApiMayChange
public @interface Experimental {
}
