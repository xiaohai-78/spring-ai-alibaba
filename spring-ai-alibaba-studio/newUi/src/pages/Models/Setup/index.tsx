/**
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Tabs } from 'antd';
import Config from './Config';
import Tool from './Tool';
import type { TabsProps } from 'antd';
import styles from './index.module.css';

export default function Setup() {
  const items: TabsProps['items'] = [
    {
      key: 'config',
      label: '配置',
      children: <Config />,
    },
    {
      key: '2',
      label: '工具',
      children: <Tool />,
    },
  ];
  return (
    <div className={styles.container}>
      <Tabs defaultActiveKey="1" items={items} />
    </div>
  );
}
