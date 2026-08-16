package io.changelens.sdk.aspect;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.sdk.annotation.Audit;
import io.changelens.sdk.audit.AuditCaptureContext;
import io.changelens.sdk.audit.AuditEventFactory;
import io.changelens.sdk.audit.AuditEventPublisher;
import io.changelens.sdk.audit.AuditSource;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.aop.support.AopUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.List;

@Aspect
@Component
@RequiredArgsConstructor
public class AuditAspect {

    private final AuditAnnotationResolver annotationResolver;
    private final AuditEventFactory auditEventFactory;
    private final AuditEventPublisher auditEventPublisher;

    @Pointcut("@annotation(io.changelens.sdk.annotation.Audit)")
    private void auditedMethod() {
    }

    @Pointcut("@within(io.changelens.sdk.annotation.Audit)")
    private void auditedService() {
    }

    @Around("auditedMethod() || auditedService()")
    public Object audit(
            ProceedingJoinPoint joinPoint) throws Throwable {

        Method method =
                ((MethodSignature) joinPoint.getSignature())
                        .getMethod();

        Class<?> targetClass = AopUtils.getTargetClass(
                        joinPoint.getTarget());

        Audit audit =
                annotationResolver.resolve(
                        method,
                        targetClass
                );

        /*
         * The pointcut matched either the method or class,
         * so an annotation should normally be present.
         */
        if (audit == null) {
            return joinPoint.proceed();
        }

        Object result = joinPoint.proceed();

        AuditCaptureContext captureContext =
                new AuditCaptureContext(
                        audit,
                        resolveSource(method),
                        method,
                        joinPoint.getTarget(),
                        joinPoint.getArgs(),
                        result,
                        List.of()
                );

        AuditEvent auditEvent = auditEventFactory.create(captureContext);
        auditEventPublisher.publish(auditEvent);

        return result;
    }

    private AuditSource resolveSource(
            Method method) {

        return method.isAnnotationPresent(Audit.class)
                ? AuditSource.METHOD
                : AuditSource.SERVICE;
    }
}