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

export declare interface Coords3D {
  x: number
  y: number
  z: number
}

export declare interface Quaternion {
  x: number
  y: number
  z: number
  w: number
}

export declare interface UIState {
  pointerIsDown: boolean
  orbitingBlocked: boolean
}

export const Scale = trait((): Coords3D => ({ x: 1, y: 1, z: 1 }))
export const Position = trait((): Coords3D => ({ x: 0, y: 0, z: 0 }))
export const Rotation = trait((): Quaternion => ({ x: 0, y: 0, z: 0, w: 1 }))
export const Mesh = trait((): THREE.Mesh => new THREE.Mesh())
export const TextureSource = trait(
  (): TexImageSource | OffscreenCanvas => new Image(),
)
export const Material = trait((): THREE.Material => new THREE.Material())
export const GlobalUIState = trait(
  (): UIState => ({
    pointerIsDown: false,
    orbitingBlocked: false,
  }),
)
export const IsVisible = trait()
export const IsActive = trait()
