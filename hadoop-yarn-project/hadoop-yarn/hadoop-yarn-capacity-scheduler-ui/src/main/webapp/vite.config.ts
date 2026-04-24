import { reactRouter } from '@react-router/dev/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig, loadEnv } from 'vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import babel from 'vite-plugin-babel';

const ReactCompilerConfig = {
  // compilationMode: 'all' is the default - compile everything
  // No longer need 'annotation' mode since all components are compatible
};

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const clusterProxyTarget = env.VITE_CLUSTER_PROXY_TARGET;

  return {
    base: '/scheduler-ui/',
    plugins: [
      tailwindcss(),
      reactRouter(),
      babel({
        filter: /\.[jt]sx?$/,
        babelConfig: {
          presets: ['@babel/preset-typescript'],
          plugins: [['babel-plugin-react-compiler', ReactCompilerConfig]],
        },
      }),
      tsconfigPaths(),
    ],
    // Proxy configuration for local development only
    // In WAR deployment, servlet filter handles API routing to ResourceManager
    server: clusterProxyTarget
      ? {
          proxy: {
            '/ws/v1/cluster': {
              target: clusterProxyTarget,
              changeOrigin: true,
              secure: false,
            },
            '/conf': {
              target: clusterProxyTarget,
              changeOrigin: true,
              secure: false,
            },
          },
        }
      : undefined,
  };
});
