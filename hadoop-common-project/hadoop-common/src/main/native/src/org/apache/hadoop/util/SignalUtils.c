#include <signal.h>
#include <jni.h>

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_util_SignalUtils_isSigIgnored
  (JNIEnv *env, jclass clazz, jint sig) {
      struct sigaction oact;
      sigaction(sig, (struct sigaction*)NULL, &oact);
      if (oact.sa_sigaction == ((void *) SIG_IGN))
           return JNI_TRUE;
      else
           return JNI_FALSE;
}