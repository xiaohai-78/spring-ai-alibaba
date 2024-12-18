import { defineConfig } from '@umijs/max';
import { DEFAULT_NAME } from './src/constants';
import { codeInspectorPlugin } from 'code-inspector-plugin';

export default defineConfig({
  title: 'site.title',
  antd: {
    configProvider: {
      theme: { cssVar: true },
    },
    // dark: true
  },
  chainWebpack(memo) {
    memo.plugin('code-inspector-plugin').use(
      codeInspectorPlugin({
        bundler: 'webpack',
      }),
    );
  },
  access: {},
  model: {},

  initialState: {},
  request: {},
  layout: {
    title: DEFAULT_NAME,
  },
  locale: {
    // 默认使用 src/locales/zh-CN.ts 作为多语言文件
    default: 'zh-CN',
    baseSeparator: '-',
    antd: true,
    useLocalStorage: true,
    title: true,
    baseNavigator: true,
  },
  routes: [
    {
      path: '/',
      redirect: '/graph',
    },
    {
      path: '/home',
      component: './Home',
      title: 'router.home',
    },
    {
      title: 'router.chatbot',
      path: '/chatbot',
      component: './Chatbot',
    },
    {
      title: 'router.agent',
      path: '/agent',
      component: './Agent',
    },
    {
      title: 'router.graph',
      path: '/graph',
      component: './Graph',
    },
    {
      hide: true,
      path: '/graph/design',
      component: './Graph/Design',
    },
  ],
  npmClient: 'pnpm',
});
