package org.mule.consulting.interceptor;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.DefaultMuleEvent;
import org.mule.NonBlockingVoidMuleEvent;
import org.mule.OptimizedRequestContext;
import org.mule.api.MessagingException;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.MuleRuntimeException;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.interceptor.Interceptor;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.transport.ReplyToHandler;
import org.mule.interceptor.AbstractEnvelopeInterceptor;
import org.mule.management.stats.ProcessingTime;
import org.mule.module.launcher.MuleApplicationClassLoader;

/**
 * <code>TimerInterceptor</code> simply times and displays the time taken to
 * process an event.
 */
public class TimerInterceptor extends AbstractEnvelopeInterceptor implements
		Interceptor {
	/**
	 * logger used by this class
	 */
	private static Log logger = LogFactory.getLog(TimerInterceptor.class);
	private static String EXTERNAL_TRANSACTION_ID_NAME = "x-transaction-id";
	private static String ORIGINAL_HTTP_RELATIVE_PATH = "original.http.relative.path";
	private static String CLIENT_ID = "client_id";
	private static String MULE_ENV = "mule.env";
	private String timerName = "timer";
	private String flowVarsList = "";

	public TimerInterceptor() {
		super();
	}

	public TimerInterceptor(MessageProcessor next, FlowConstruct fc) {
		setListener(next);
		setFlowConstruct(fc);
	}

	@Override
	protected MuleEvent processBlocking(MuleEvent event) throws MuleException {
		logger.debug("processBlocking");
		long startTime = System.currentTimeMillis();
		ProcessingTime time = event.getProcessingTime();
		boolean exceptionWasThrown = true;

		String transactionId;

		transactionId = event.getMessage().getInboundProperty(
				EXTERNAL_TRANSACTION_ID_NAME);
		if (transactionId == null) {
			transactionId = event.getMessage().getInvocationProperty(
					EXTERNAL_TRANSACTION_ID_NAME);
		}
		if (transactionId == null) {
			transactionId = UUID.randomUUID().toString();
			if (logger.isInfoEnabled()) {
				logger.info("generated transaction id {\"transactionId\":\""
						+ transactionId + "\", " + "\"event\":\"" + event.getId()
						+ "\"" + "}");
			}
		}

		try {
			event.getMessage().setOutboundProperty(
					EXTERNAL_TRANSACTION_ID_NAME, transactionId);
		} catch (Exception ignored) {
			logger.debug("Setting outbound property "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}


		try {
			event.getMessage().setInvocationProperty(
					ORIGINAL_HTTP_RELATIVE_PATH, event.getMessage().getInboundProperty("http.relative.path"));
		} catch (Exception ignored) {
			logger.debug("Setting flowVar "
					+ ORIGINAL_HTTP_RELATIVE_PATH + ": "
					+ ignored.getMessage());
		}

		try {
			event.getMessage().setInvocationProperty(
					EXTERNAL_TRANSACTION_ID_NAME, transactionId);
		} catch (Exception ignored) {
			logger.debug("Setting flowVar "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}
		
		try {
			if (logger.isInfoEnabled()) {
				String appName = ((MuleApplicationClassLoader)this.getClass().getClassLoader()).getAppName();
				logger.info(appName + " enter " 
						+ event.getFlowConstruct().getName() + " {" 
						+ flowVar_json(event, CLIENT_ID)
						+ flowVar_json(event, MULE_ENV)
						+ "\"httpPath\":\"" 
						+ event.getMessage().getInvocationProperty(ORIGINAL_HTTP_RELATIVE_PATH) + "\", "
						+ flowVar_json(event, flowVarsList)
						+ flowVar_json(event, EXTERNAL_TRANSACTION_ID_NAME)
						+ "\"event\":\"" 
						+ event.getId() + "\"" + "}");
			}
		} catch (Exception ignored) {
			logger.debug("TimerInterceptor.processBlock() caught Exception "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}
		
		MuleEvent resultEvent = event;
		
		try {
			resultEvent = after(processNext(before(resultEvent)));
			exceptionWasThrown = false;
		} finally {
			resultEvent = last(resultEvent, time, startTime, exceptionWasThrown);
		}
		return resultEvent;
	}

	@Override
	protected MuleEvent processNonBlocking(final MuleEvent event)
			throws MuleException {
		logger.debug("processNonBlocking");
		final long startTime = System.currentTimeMillis();
		final ProcessingTime time = event.getProcessingTime();

		String transactionId;

		transactionId = event.getMessage().getInboundProperty(
				EXTERNAL_TRANSACTION_ID_NAME);
		if (transactionId == null) {
			transactionId = event.getMessage().getInvocationProperty(
					EXTERNAL_TRANSACTION_ID_NAME);
		}
		if (transactionId == null) {
			transactionId = UUID.randomUUID().toString();
			if (logger.isInfoEnabled()) {
				logger.info("generated transaction id {\"transactionId\":\""
						+ transactionId + "\", " + "\"event\":\"" + event.getId()
						+ "\"" + "}");
			}
		}

		try {
			event.getMessage().setOutboundProperty(
					EXTERNAL_TRANSACTION_ID_NAME, transactionId);
		} catch (Exception ignored) {
			logger.debug("Setting outbound property "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}

		try {
			event.getMessage().setInvocationProperty(
					ORIGINAL_HTTP_RELATIVE_PATH, event.getMessage().getInboundProperty("http.relative.path"));
		} catch (Exception ignored) {
			logger.debug("Setting flowVar "
					+ ORIGINAL_HTTP_RELATIVE_PATH + ": "
					+ ignored.getMessage());
		}

		try {
			event.getMessage().setInvocationProperty(
					EXTERNAL_TRANSACTION_ID_NAME, transactionId);
		} catch (Exception ignored) {
			logger.debug("Setting flowVar "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}
		
		try {
			if (logger.isInfoEnabled()) {
				String appName = ((MuleApplicationClassLoader)this.getClass().getClassLoader()).getAppName();
				logger.info(appName + " enter " 
						+ event.getFlowConstruct().getName() + " {" 
						+ flowVar_json(event, CLIENT_ID)
						+ flowVar_json(event, MULE_ENV)
						+ "\"httpPath\":\"" 
						+ event.getMessage().getInvocationProperty(ORIGINAL_HTTP_RELATIVE_PATH) + "\", "
						+ flowVar_json(event, flowVarsList)
						+ flowVar_json(event, EXTERNAL_TRANSACTION_ID_NAME)
						+ "\"event\":\"" 
						+ event.getId() + "\"" + "}");
			}
		} catch (Exception ignored) {
			logger.debug("TimerInterceptor.processBlock() caught Exception "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}
		
		MuleEvent responseEvent = event;

		final ReplyToHandler originalReplyToHandler = event.getReplyToHandler();
		responseEvent = new DefaultMuleEvent(event, new TimerInterceptorResponseReplyToHandler(
				originalReplyToHandler, time, startTime));
		// Update RequestContext ThreadLocal for backwards compatibility
		OptimizedRequestContext.unsafeSetEvent(responseEvent);

		try {
			responseEvent = processNext(processRequest(responseEvent));
			if (!(responseEvent instanceof NonBlockingVoidMuleEvent)) {
				responseEvent = processResponse(responseEvent);
			}
			last(responseEvent, time, startTime, true);
		} catch (MuleException exception) {
			last(responseEvent, time, startTime, true);
			throw exception;
		}
		return responseEvent;
	}

	public String getTimerName() {
		return timerName;
	}

	public void setTimerName(String timerName) {
		this.timerName = timerName;
	}

	public String getFlowVarsList() {
		return flowVarsList;
	}

	public void setFlowVarsList(String flowVarsList) {
		this.flowVarsList = flowVarsList;
	}
	
	protected String flowVar_json(final MuleEvent event, String varname_list)
			throws MuleException {
		StringBuilder sb = new StringBuilder();
		String[] varnames = varname_list.split(",");
		for (String varname: varnames) {
			String val = event.getMessage().getInvocationProperty(varname);
			if (val != null) {
				sb.append("\"").append(varname).append("\":");
				sb.append("\"").append(val).append("\",");
			}
		}
		return sb.toString(); 
	}

	@Override
	public MuleEvent after(MuleEvent event) throws MuleException {
		logger.debug("after");
		return event;
	}

	@Override
	public MuleEvent before(MuleEvent event) throws MuleException {
		logger.debug("before");
		return event;
	}

	@Override
	public MuleEvent last(MuleEvent event, ProcessingTime time, long startTime,
			boolean exceptionWasThrown) throws MuleException {
		logger.debug("last");
		try {
			event.getMessage().setOutboundProperty(
					EXTERNAL_TRANSACTION_ID_NAME, event.getMessage().getInvocationProperty(EXTERNAL_TRANSACTION_ID_NAME));
		
			if (logger.isInfoEnabled()) {
				long elapsedTime = System.currentTimeMillis() - startTime;
				String appName = ((MuleApplicationClassLoader)this.getClass().getClassLoader()).getAppName();
				logger.info(appName + " exit " 
						+ event.getFlowConstruct().getName() + " {\"timerName\":\"" + timerName + "\", "
						+ "\"elapsedMS\":" + elapsedTime + ", " 
						+ flowVar_json(event, CLIENT_ID)
						+ flowVar_json(event, MULE_ENV)
						+ "\"httpPath\":\"" 
						+ event.getMessage().getInvocationProperty(ORIGINAL_HTTP_RELATIVE_PATH) + "\", "
						+ flowVar_json(event, flowVarsList)
						+ flowVar_json(event, EXTERNAL_TRANSACTION_ID_NAME)
						+ "\"event\":\"" 
						+ event.getId() + "\"" + "}");
			}
					
		} catch (Exception ignored) {
			logger.debug("TimerInterceptor.last() caught Exception "
					+ EXTERNAL_TRANSACTION_ID_NAME + ": "
					+ ignored.getMessage());
		}

		return event;
	}

	class TimerInterceptorResponseReplyToHandler implements ReplyToHandler {

		private final ReplyToHandler originalReplyToHandler;
		private final ProcessingTime time;
		private final long startTime;

		public TimerInterceptorResponseReplyToHandler(ReplyToHandler originalReplyToHandler,
				ProcessingTime time, long startTime) {
			this.originalReplyToHandler = originalReplyToHandler;
			this.time = time;
			this.startTime = startTime;
		}

		public void processReplyTo(final MuleEvent event,
				MuleMessage returnMessage, Object replyTo) throws MuleException {
			MuleEvent response = event;
			boolean exceptionWasThrown = true;
			try {
				response = after(event);
				originalReplyToHandler.processReplyTo(response, null, replyTo);
				exceptionWasThrown = false;
			} finally {
				last(response, time, startTime, false);
			}
		}

		public void processExceptionReplyTo(MessagingException exception,
				Object replyTo) {
			try {
				originalReplyToHandler.processExceptionReplyTo(exception,
						replyTo);
			} finally {
				try {
					last(exception.getEvent(), time, startTime, true);
				} catch (MuleException muleException) {
					throw new MuleRuntimeException(muleException);
				}
			}
		}
	}
}