/**
 * Copyright 2026 The Action Engine Authors.
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

import { trait } from 'koota'
import * as THREE from 'three'

export type PositionArray = [x: number, y: number, z: number]
export const PositionTrait = trait((): PositionArray => [0, 0, 0])

export const RotationTrait = trait((): Array<number> => [0, 0, 0])
export const ScaleTrait = trait((): Array<number> => [1, 1, 1])

export const Color = trait((): string => '#ffffff')

export const IsSelected = trait()
